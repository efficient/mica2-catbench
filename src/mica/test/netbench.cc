#include <atomic>
#include <sys/mman.h>

#include "mica/datagram/datagram_client.h"
#include "mica/util/lcore.h"
#include "mica/util/hash.h"
#include "mica/util/zipf.h"
#include "mica/util/rate_limiter.h"

using std::atomic;
using std::atomic_fetch_add;

#define ITERATIONS 30000000
#define WARMUP 10000000

typedef ::mica::alloc::HugeTLBFS_SHM Alloc;

static ::mica::util::Stopwatch sw;
static uint64_t* latencies;
static atomic<uint64_t> num_latencies;

struct DPDKConfig : public ::mica::network::BasicDPDKConfig {
  static constexpr bool kVerbose = true;
};

struct DatagramClientConfig
    : public ::mica::datagram::BasicDatagramClientConfig {
  typedef struct ArgumentStruct {
   private:
    static atomic<uint64_t> watermark;

   public:
    uint64_t kId;
    uint64_t kTs;

    inline ArgumentStruct(bool actually_do_the_thing = false)
        : kId(actually_do_the_thing ? atomic_fetch_add(&watermark, 1ul) : -1ul),
          kTs(sw.now()) {}
  } Argument;

  typedef ::mica::network::DPDK<DPDKConfig> Network;
  // static constexpr bool kSkipRX = true;
  // static constexpr bool kIgnoreServerPartition = true;
  // static constexpr bool kVerbose = true;
};

atomic<uint64_t> DatagramClientConfig::ArgumentStruct::watermark(0);

typedef ::mica::datagram::DatagramClient<DatagramClientConfig> Client;

typedef ::mica::table::Result Result;

template <typename T>
static uint64_t hash(const T* key, size_t key_length) {
  return ::mica::util::hash(key, key_length);
}

class ResponseHandler
    : public ::mica::datagram::ResponseHandlerInterface<Client> {
 public:
  void handle(Client::RequestDescriptor rd, Result result, const char* value,
              size_t value_length, const Argument& arg) {
    (void)rd;
    (void)result;
    (void)value;
    (void)value_length;
    latencies[arg.kId] = sw.diff_in_us(sw.now(), arg.kTs);
    atomic_fetch_add(&num_latencies, 1ul);
  }
};

struct Args {
  size_t actual_lcore_count;
  uint16_t lcore_id;
  ::mica::util::Config* config;
  Alloc* alloc;
  Client* client;

  uint64_t num_items;
  double get_ratio;
  double zipf_theta;
  double tput_limit;
} __attribute__((aligned(128)));

int worker_proc(void* arg) {
  auto args = reinterpret_cast<Args*>(arg);

  Client& client = *args->client;

  ::mica::util::lcore.pin_thread(args->lcore_id);

  printf("worker running on lcore %" PRIu16 "\n", args->lcore_id);

  client.probe_reachability();

  ResponseHandler rh;

  uint32_t get_threshold = (uint32_t)(args->get_ratio * (double)((uint32_t)-1));

  ::mica::util::Rand op_type_rand(static_cast<uint64_t>(args->lcore_id) + 1000);
  ::mica::util::ZipfGen zg(args->num_items, args->zipf_theta,
                           static_cast<uint64_t>(args->lcore_id));
  bool limit_tput = args->tput_limit > 0.;
  ::mica::util::RateLimiter rate_limiter(
      sw, 0., 1000.,
      args->tput_limit * 1000000. / static_cast<double>(sw.c_1_sec()));

  uint64_t key_i;
  uint64_t key_hash;
  size_t key_length = sizeof(key_i);
  char* key = reinterpret_cast<char*>(&key_i);

  uint64_t value_i;
  size_t value_length = sizeof(value_i);
  char* value = reinterpret_cast<char*>(&value_i);

  bool use_noop = false;
  // bool use_noop = true;

  uint64_t last_handle_response_time = sw.now();
  // Check the response after sending some requests.
  // Ideally, packets per batch for both RX and TX should be similar.
  uint64_t response_check_interval = 20 * sw.c_1_usec();

  for (uint64_t seq = 0; seq < ITERATIONS; seq += args->actual_lcore_count) {
    // Determine the operation type.
    uint32_t op_r = op_type_rand.next_u32();
    bool is_get = op_r <= get_threshold;

    // Generate the key.
    key_i = zg.next();
    key_hash = hash(key, key_length);

    uint64_t now = sw.now();
    while (!client.can_request(key_hash) ||
           sw.diff_in_cycles(now, last_handle_response_time) >=
               response_check_interval ||
           (limit_tput && !rate_limiter.try_remove_tokens(1.))) {
      last_handle_response_time = now;
      client.handle_response(rh);
      now = sw.now();
    }

    if (!use_noop) {
      if (is_get)
        client.get(key_hash, key, key_length, {true});
      else {
        value_i = seq;
        client.set(key_hash, key, key_length, value, value_length, true,
                   {true});
      }
    } else {
      if (is_get)
        client.noop_read(key_hash, key, key_length, {true});
      else {
        value_i = seq;
        client.noop_write(key_hash, key, key_length, value, value_length,
                          {true});
      }
    }
  }

  while (num_latencies < ITERATIONS) client.handle_response(rh);

  return 0;
}

int main(int argc, const char* argv[]) {
  if (argc != 5) {
    printf("%s NUM-ITEMS GET-RATIO ZIPF-THETA TPUT-LIMIT(M req/sec)\n",
           argv[0]);
    return EXIT_FAILURE;
  }

  uint64_t num_items = static_cast<uint64_t>(atol(argv[1]));
  double get_ratio = atof(argv[2]);
  double zipf_theta = atof(argv[3]);
  double tput_limit = atof(argv[4]);
  printf("num_items=%" PRIu64 "\n", num_items);
  printf("get_ratio=%lf\n", get_ratio);
  printf("zipf_theta=%lf\n", zipf_theta);
  printf("tput_limit=%lf\n", tput_limit);

  ::mica::util::lcore.pin_thread(0);

  auto config = ::mica::util::Config::load_file("netbench.json");

  latencies = reinterpret_cast<uint64_t*>(
      mmap(nullptr, ITERATIONS * sizeof *latencies, PROT_READ | PROT_WRITE,
           MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0));
  if (!latencies || latencies == MAP_FAILED) {
    perror("Allocating simply gynormous array");
    return 1;
  }
  sw.init_start();
  sw.init_end();

  Alloc alloc(config.get("alloc"));

  DatagramClientConfig::Network network(config.get("network"));
  network.start();

  Client::DirectoryClient dir_client(config.get("dir_client"));

  Client client(config.get("client"), &network, &dir_client);
  client.discover_servers();

  uint16_t lcore_count =
      static_cast<uint16_t>(::mica::util::lcore.lcore_count());
  size_t actual_lcore_count = config.get("network").get("lcores").size();

  std::vector<Args> args(lcore_count);
  for (uint16_t lcore_id = 0; lcore_id < lcore_count; lcore_id++) {
    args[lcore_id].actual_lcore_count = actual_lcore_count;
    args[lcore_id].lcore_id = lcore_id;
    args[lcore_id].config = &config;
    args[lcore_id].alloc = &alloc;
    args[lcore_id].client = &client;
    args[lcore_id].num_items = num_items;
    args[lcore_id].get_ratio = get_ratio;
    args[lcore_id].zipf_theta = zipf_theta;
    args[lcore_id].tput_limit =
        tput_limit / static_cast<double>(actual_lcore_count);
  }

  for (uint16_t lcore_id = 1; lcore_id < lcore_count; lcore_id++) {
    if (!rte_lcore_is_enabled(static_cast<uint8_t>(lcore_id))) continue;
    rte_eal_remote_launch(worker_proc, &args[lcore_id], lcore_id);
  }
  worker_proc(&args[0]);
  rte_eal_mp_wait_lcore();

  double ave = 0;
  for (uint64_t each = 0; each < ITERATIONS; ++each) {
    uint64_t lat = latencies[each];
    printf("Completed after: %ld us\n", lat);
    if (each >= WARMUP) ave += static_cast<double>(lat);
  }
  ave /= ITERATIONS - WARMUP;
  printf("Average: %f us\n", ave);
  fflush(stdout);

  munmap(latencies, ITERATIONS * sizeof *latencies);
  network.stop();

  return EXIT_SUCCESS;
}
