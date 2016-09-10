#include <sys/mman.h>
#include <unistd.h>
#include <atomic>

#include "mica/datagram/datagram_client.h"
#include "mica/util/hash.h"
#include "mica/util/latency.h"
#include "mica/util/lcore.h"
#include "mica/util/rate_limiter.h"
#include "mica/util/zipf.h"

using std::atomic;
using std::atomic_fetch_add;

typedef ::mica::alloc::HugeTLBFS_SHM Alloc;

static ::mica::util::Stopwatch sw;

static atomic<uint64_t> iteration_start;
static atomic<bool> reset_stats;
static atomic<bool> stopping;

struct DPDKConfig : public ::mica::network::BasicDPDKConfig {
  static constexpr bool kVerbose = true;
};

struct DatagramClientConfig
    : public ::mica::datagram::BasicDatagramClientConfig {
  typedef struct ArgumentStruct {
   public:
    uint64_t ts;

    ArgumentStruct() : ts(sw.now()) {}
  } Argument;

  typedef ::mica::network::DPDK<DPDKConfig> Network;
  // static constexpr bool kSkipRX = true;
  // static constexpr bool kIgnoreServerPartition = true;
  // static constexpr bool kVerbose = true;
};

typedef ::mica::datagram::DatagramClient<DatagramClientConfig> Client;

typedef ::mica::table::Result Result;

template <typename T>
static uint64_t hash(const T* key, size_t key_length) {
  return ::mica::util::hash(key, key_length);
}

class ResponseHandler
    : public ::mica::datagram::ResponseHandlerInterface<Client> {
 public:
  ResponseHandler(::mica::util::Latency& lt) : lt_(lt) {}

  void handle(Client::RequestDescriptor rd, Result result, const char* value,
              size_t value_length, const Argument& arg) {
    (void)rd;
    (void)result;
    (void)value;
    (void)value_length;
    lt_.update(sw.diff_in_us(sw.now(), arg.ts));
  }

 private:
  ::mica::util::Latency& lt_;
};

struct Args {
  size_t actual_lcore_count;
  size_t lcore_count;
  uint16_t lcore_id;
  ::mica::util::Config* config;
  Alloc* alloc;
  Client* client;

  ::mica::util::Latency* lt_arr;

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
  // size_t max_iterations = args->max_iterations;

  ::mica::util::lcore.pin_thread(args->lcore_id);

  printf("worker running on lcore %" PRIu16 "\n", args->lcore_id);

  client.probe_reachability();

  ResponseHandler rh(args->lt_arr[args->lcore_id]);

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

  uint64_t last_report_time = sw.now() - sw.c_1_sec();

  for (uint64_t seq = 0; !stopping; seq++) {
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
        client.get(key_hash, key, key_length, {});
      else {
        value_i = seq;
        client.set(key_hash, key, key_length, value, value_length, true, {});
      }
    } else {
      if (is_get)
        client.noop_read(key_hash, key, key_length, {});
      else {
        value_i = seq;
        client.noop_write(key_hash, key, key_length, value, value_length, {});
      }
    }

    if (args->lcore_id == 0 && (seq & 0xffff) == 0 &&
        sw.diff_in_us(now, last_report_time) >= 1000000) {
      last_report_time = now;

      ::mica::util::Latency lt;
      // This assumes that lcore IDs are consecutive.
      for (size_t i = 0; i < args->lcore_count; i++)
        lt += args->lt_arr[i];

      if (reset_stats) {
        reset_stats = false;
        args->lt_arr[args->lcore_count] = lt;
        args->lt_arr[args->lcore_count + 1] = lt;
      }

      ::mica::util::Latency lt_diff = lt;
      lt_diff -= args->lt_arr[args->lcore_count];

      printf("Interval:   ");
      printf("Cnt %9" PRIu64 ", ", lt_diff.count());
      printf("Avg %7.2lf us, ", lt_diff.avg_f());
      printf("Min %4" PRIu64 " us, ", lt_diff.min());
      printf("Max %4" PRIu64 " us, ", lt_diff.max());
      printf("50-th: %4" PRIu64 " us, ", lt_diff.perc(0.5));
      printf("90-th: %4" PRIu64 " us, ", lt_diff.perc(0.9));
      printf("95-th: %4" PRIu64 " us, ", lt_diff.perc(0.95));
      printf("99-th: %4" PRIu64 " us, ", lt_diff.perc(0.99));
      printf("99.9-th: %4" PRIu64 " us\n", lt_diff.perc(0.999));

      lt_diff = lt;
      lt_diff -= args->lt_arr[args->lcore_count + 1];

      printf("Cumulative: ");
      printf("Cnt %9" PRIu64 ", ", lt_diff.count());
      printf("Avg %7.2lf us, ", lt_diff.avg_f());
      printf("Min %4" PRIu64 " us, ", lt_diff.min());
      printf("Max %4" PRIu64 " us, ", lt_diff.max());
      printf("50-th: %4" PRIu64 " us, ", lt_diff.perc(0.5));
      printf("90-th: %4" PRIu64 " us, ", lt_diff.perc(0.9));
      printf("95-th: %4" PRIu64 " us, ", lt_diff.perc(0.95));
      printf("99-th: %4" PRIu64 " us, ", lt_diff.perc(0.99));
      printf("99.9-th: %4" PRIu64 " us\n", lt_diff.perc(0.999));

      printf("\n");
      fflush(stdout);

      args->lt_arr[args->lcore_count] = lt;
    }
  }

  return 0;
}

int controller_proc(void* arg) {
  (void)arg;

  char buf[1024];
  while (true) {
    if (fgets(buf, sizeof(buf), stdin) == nullptr) break;

    if (strcmp(buf, "r\n") == 0) {
      reset_stats = true;
    } else if (strcmp(buf, "s\n") == 0) {
      stopping = true;
      break;
    }
  }
  return 0;
}

int main(int argc, const char* argv[]) {
  // FILE* latency_out_file = stdout;
  // bool out_to_dev_null = false;
  size_t max_iterations = 100000000;  // 100 Mi
  size_t subsample_factor = 1;
  int tput_limit_mode = 0;

  int c;
  opterr = 0;
  while ((c = getopt(argc, const_cast<char**>(argv), "o:n:s:p")) != -1) {
    switch (c) {
      case 'o':
        // if (strcmp(optarg, "-") == 0)
        //   latency_out_file = stdout;
        // else {
        //   if (strcmp(optarg, "/dev/null") == 0)
        //     out_to_dev_null = true;
        //   else
        //     latency_out_file = fopen(optarg, "w");
        // }
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

  auto lt_arr = new ::mica::util::Latency[lcore_count + 2];

  std::vector<Args> args(lcore_count);
  for (uint16_t lcore_id = 0; lcore_id < lcore_count; lcore_id++) {
    args[lcore_id].actual_lcore_count = actual_lcore_count;
    args[lcore_id].lcore_count = lcore_count;
    args[lcore_id].lcore_id = lcore_id;
    args[lcore_id].config = &config;
    args[lcore_id].alloc = &alloc;
    args[lcore_id].client = &client;
    args[lcore_id].lt_arr = lt_arr;
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
  printf("  s: stop measurement\n");

  for (uint16_t lcore_id = 1; lcore_id < lcore_count; lcore_id++) {
    if (!rte_lcore_is_enabled(static_cast<uint8_t>(lcore_id))) continue;
    rte_eal_remote_launch(worker_proc, &args[lcore_id], lcore_id);
  }
  worker_proc(&args[0]);
  rte_eal_mp_wait_lcore();

  ctrl_thd.join();

  ::mica::util::Latency lt = lt_arr[lcore_count];
  lt -= lt_arr[lcore_count + 1];

  printf("Average: %.2lf us\n", lt.avg_f());
  printf("Minimum: %" PRIu64 " us\n", lt.min());
  printf("Maximum: %" PRIu64 " us\n", lt.max());
  printf("50-th: %" PRIu64 " us\n", lt.perc(0.5));
  printf("90-th: %" PRIu64 " us\n", lt.perc(0.9));
  printf("95-th: %" PRIu64 " us\n", lt.perc(0.95));
  printf("99-th: %" PRIu64 " us\n", lt.perc(0.99));
  printf("99.9-th: %" PRIu64 " us\n", lt.perc(0.999));
  fflush(stdout);

  delete[] lt_arr;

  network.stop();

  return EXIT_SUCCESS;
}
