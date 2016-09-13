#include <atomic>
#include <sys/mman.h>
#include <unistd.h>

#include "mica/datagram/datagram_client.h"
#include "mica/util/lcore.h"
#include "mica/util/hash.h"
#include "mica/util/zipf.h"
#include "mica/util/latency.h"
#include "mica/util/rate_limiter.h"

using std::atomic;
using std::atomic_fetch_add;

typedef ::mica::alloc::HugeTLBFS_SHM Alloc;

static ::mica::util::Stopwatch sw;
static uint64_t* latencies;
static atomic<uint64_t> num_latencies;

static atomic<uint64_t> iteration_start;
static atomic<bool> stopping;
static atomic<uint64_t> delay_until;

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

    static uint64_t get_current_watermark() { return watermark; }
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
  ResponseHandler(uint64_t max_iterations, uint64_t* pending)
      : max_iterations_(max_iterations), pending_(pending) {}

  void handle(Client::RequestDescriptor rd, Result result, const char* value,
              size_t value_length, const Argument& arg) {
    (void)rd;
    (void)result;
    (void)value;
    (void)value_length;
    latencies[arg.kId % max_iterations_] = sw.diff_in_us(sw.now(), arg.kTs);
    atomic_fetch_add(&num_latencies, 1ul);
    (*pending_)--;
  }

 private:
  uint64_t max_iterations_;
  uint64_t* pending_;
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
  int tput_limit_mode;
  double tput_limit;

  size_t max_iterations;
} __attribute__((aligned(128)));

int worker_proc(void* arg) {
  auto args = reinterpret_cast<Args*>(arg);

  Client& client = *args->client;
  size_t max_iterations = args->max_iterations;

  ::mica::util::lcore.pin_thread(args->lcore_id);

  printf("worker running on lcore %" PRIu16 "\n", args->lcore_id);

  client.probe_reachability();

  uint64_t pending = 0;
  ResponseHandler rh(max_iterations, &pending);

  uint32_t get_threshold = (uint32_t)(args->get_ratio * (double)((uint32_t)-1));

  ::mica::util::Rand op_type_rand(static_cast<uint64_t>(args->lcore_id) + 1000);
  ::mica::util::ZipfGen zg(args->num_items, args->zipf_theta,
                           static_cast<uint64_t>(args->lcore_id));

  bool limit_tput = args->tput_limit > 0.;
  int tput_limit_mode = args->tput_limit_mode;
  ::mica::util::RegularRateLimiter reg_rate_limiter(
      sw, 0., 1000.,
      args->tput_limit * 1000000. / static_cast<double>(sw.c_1_sec()));
  ::mica::util::ExponentialRateLimiter exp_rate_limiter(
      sw, 0., 1000.,
      args->tput_limit * 1000000. / static_cast<double>(sw.c_1_sec()),
      static_cast<uint64_t>(args->lcore_id) + 2000);

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

  for (uint64_t seq = 0; !stopping; seq += args->actual_lcore_count) {
    if (delay_until != 0)
	while (static_cast<int64_t>(delay_until - sw.now()) > 0)
	   client.handle_response(rh);

    // Determine the operation type.
    uint32_t op_r = op_type_rand.next_u32();
    bool is_get = op_r <= get_threshold;

    // Generate the key.
    key_i = zg.next();
    key_hash = hash(key, key_length);

    uint64_t now = sw.now();
    while (
        !client.can_request(key_hash) ||
        sw.diff_in_cycles(now, last_handle_response_time) >=
            response_check_interval ||
        (limit_tput &&
         ((tput_limit_mode == 0 && !reg_rate_limiter.try_remove_tokens(1.)) ||
          (tput_limit_mode == 1 && !exp_rate_limiter.try_remove_tokens(1.))))) {
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
    pending++;
  }

  while (pending != 0) client.handle_response(rh);

  return 0;
}

int controller_proc(void* arg) {
  (void)arg;

  char buf[1024];
  while (true) {
    if (fgets(buf, sizeof(buf), stdin) == nullptr) break;

    if (strcmp(buf, "r\n") == 0)
      iteration_start =
          DatagramClientConfig::ArgumentStruct::get_current_watermark();
    else if (strcmp(buf, "d\n") == 0)
      delay_until = sw.now() + sw.c_1_sec();
    else if (strcmp(buf, "s\n") == 0) {
      stopping = true;
      break;
    }
  }
  return 0;
}

int main(int argc, const char* argv[]) {
  FILE* latency_out_file = stdout;
  bool out_to_dev_null = false;
  size_t max_iterations = 100000000;  // 100 Mi
  size_t subsample_factor = 1;
  int tput_limit_mode = 0;

  int c;
  opterr = 0;
  while ((c = getopt(argc, const_cast<char**>(argv), "o:n:s:p")) != -1) {
    switch (c) {
      case 'o':
        if (strcmp(optarg, "-") == 0)
          latency_out_file = stdout;
        else {
          if (strcmp(optarg, "/dev/null") == 0)
            out_to_dev_null = true;
          else
            latency_out_file = fopen(optarg, "w");
        }
        break;
      case 'n':
        max_iterations = static_cast<size_t>(atol(optarg));
        break;
      case 's':
        subsample_factor = static_cast<size_t>(atol(optarg));
        break;
      case 'p':
        tput_limit_mode = 1;
        break;
      case '?':
        if (isprint(optopt))
          fprintf(stderr, "incomplete option -%c\n", optopt);
        else
          fprintf(stderr, "error parsing arguments\n");
        return EXIT_FAILURE;
    }
  }

  if (argc - optind != 4) {
    printf(
        "%s [-o LATENCY-OUT-FILENAME] [-n MAX-ITERATIONS] [-s "
        "SUBSAMPLE-FACTOR] [-p] NUM-ITEMS GET-RATIO ZIPF-THETA "
        "TPUT-LIMIT(M req/sec)\n",
        argv[0]);
    return EXIT_FAILURE;
  }

  uint64_t num_items = static_cast<uint64_t>(atol(argv[optind + 0]));
  double get_ratio = atof(argv[optind + 1]);
  double zipf_theta = atof(argv[optind + 2]);
  double tput_limit = atof(argv[optind + 3]);
  printf("num_items=%" PRIu64 "\n", num_items);
  printf("get_ratio=%lf\n", get_ratio);
  printf("zipf_theta=%lf\n", zipf_theta);
  printf("tput_limit=%lf\n", tput_limit);
  printf("tput_limit_mode=%d (%s)\n", tput_limit_mode,
         tput_limit_mode == 0 ? "regular" : "poisson");
  printf("max_iterations=%zu\n", max_iterations);
  printf("subsample_factor=%zu\n", subsample_factor);

  ::mica::util::lcore.pin_thread(0);

  auto config = ::mica::util::Config::load_file("netbench.json");

  latencies = reinterpret_cast<uint64_t*>(
      mmap(nullptr, max_iterations * sizeof(*latencies), PROT_READ | PROT_WRITE,
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
    args[lcore_id].tput_limit_mode = tput_limit_mode;
    args[lcore_id].tput_limit =
        tput_limit / static_cast<double>(actual_lcore_count);
    args[lcore_id].max_iterations = max_iterations;
  }

  iteration_start = 0;
  stopping = false;
  std::thread ctrl_thd(controller_proc, nullptr);

  printf("control commands:\n");
  printf("  r: reset measurement\n");
  printf("  d: delay sending requests for 1 second\n");
  printf("  s: stop measurement\n");

  for (uint16_t lcore_id = 1; lcore_id < lcore_count; lcore_id++) {
    if (!rte_lcore_is_enabled(static_cast<uint8_t>(lcore_id))) continue;
    rte_eal_remote_launch(worker_proc, &args[lcore_id], lcore_id);
  }
  worker_proc(&args[0]);
  rte_eal_mp_wait_lcore();

  ctrl_thd.join();

  uint64_t iteration_start_nv = iteration_start;
  auto iteration_end_nv =
      DatagramClientConfig::ArgumentStruct::get_current_watermark();

  printf("issued:   %" PRIu64 " requests [%" PRIu64 ", %" PRIu64 ")\n",
         iteration_end_nv - iteration_start_nv, iteration_start_nv,
         iteration_end_nv);

  uint64_t iterations;
  if (iteration_end_nv - iteration_start_nv <= max_iterations)
    iterations = iteration_end_nv - iteration_start_nv;
  else {
    iteration_start_nv = iteration_end_nv - max_iterations;
    iterations = max_iterations;
  }
  printf("analyzed: %" PRIu64 " requests [%" PRIu64 ", %" PRIu64 ")\n",
         iteration_end_nv - iteration_start_nv, iteration_start_nv,
         iteration_end_nv);

  size_t subsample_c = subsample_factor;
  size_t subsample_i = 0;
  ::mica::util::Rand subsample_rand(sw.now() % (uint64_t(1) << 32));
  ::mica::util::Latency lt;
  for (uint64_t each = 0;
       each < iterations / subsample_factor * subsample_factor; ++each) {
    uint64_t lat = latencies[(each + iteration_start_nv) % max_iterations];
    if (subsample_c == subsample_factor) {
      subsample_c = 0;
      subsample_i = each + subsample_rand.next_u32() % subsample_factor;
    }
    if (!out_to_dev_null && each == subsample_i)
      fprintf(latency_out_file, "Completed after: %ld us\n", lat);
    subsample_c++;
    lt.update(lat);
  }
  if (!out_to_dev_null && latency_out_file != stdout &&
      latency_out_file != stderr)
    fclose(latency_out_file);

  printf("Average: %.2lf us\n", lt.avg_f());
  printf("Minimum: %" PRIu64 " us\n", lt.min());
  printf("Maximum: %" PRIu64 " us\n", lt.max());
  printf("50-th: %" PRIu64 " us\n", lt.perc(0.5));
  printf("90-th: %" PRIu64 " us\n", lt.perc(0.9));
  printf("95-th: %" PRIu64 " us\n", lt.perc(0.95));
  printf("99-th: %" PRIu64 " us\n", lt.perc(0.99));
  printf("99.9-th: %" PRIu64 " us\n", lt.perc(0.999));
  fflush(stdout);

  munmap(latencies, max_iterations * sizeof(*latencies));
  network.stop();

  return EXIT_SUCCESS;
}
