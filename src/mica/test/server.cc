#include "mica/datagram/datagram_server.h"
#include "mica/util/lcore.h"
#include "mica/util/hash.h"

struct DPDKConfig : public ::mica::network::BasicDPDKConfig {
  static constexpr bool kVerbose = true;
  static constexpr uint16_t kRXDescCount = 256;
  static constexpr uint16_t kSpareMBufCount = 6144 - kRXDescCount - kTXDescCount;
};

struct PartitionsConfig : public ::mica::processor::BasicPartitionsConfig {
  static constexpr bool kSkipPrefetchingForRecentKeyHashes = false;
  // static constexpr bool kVerbose = true;
};

struct DatagramServerConfig
    : public ::mica::datagram::BasicDatagramServerConfig {
  typedef ::mica::processor::Partitions<PartitionsConfig> Processor;
  typedef ::mica::network::DPDK<DPDKConfig> Network;
  // static constexpr bool kVerbose = true;
};

typedef ::mica::datagram::DatagramServer<DatagramServerConfig> Server;

typedef DatagramServerConfig::Processor Processor;
typedef ::mica::table::Result Result;
typedef ::mica::processor::Operation Operation;

template <typename T>
static uint64_t hash(const T* key, size_t key_length) {
  return ::mica::util::hash(key, key_length);
}

int main(int argv, const char* argc[]) {
  ::mica::util::lcore.pin_thread(0);

  auto config = ::mica::util::Config::load_file("server.json");

  Server::DirectoryClient dir_client(config.get("dir_client"));

  DatagramServerConfig::Processor::Alloc alloc(config.get("alloc"));
  DatagramServerConfig::Processor processor(config.get("processor"), &alloc);

  // Prepopulate the table.
  uint64_t prepopulation_key_count = 0;
  if (argv >= 2) prepopulation_key_count = static_cast<uint64_t>(atoi(argc[1]));

  if (prepopulation_key_count != 0) {
    uint64_t key_i;
    uint64_t value_i;
    const char* key = reinterpret_cast<const char*>(&key_i);
    char* value = reinterpret_cast<char*>(&value_i);
    size_t key_length = sizeof(key_i);
    size_t value_length = sizeof(value_i);

    Result out_result;
    size_t out_value_length;

    Operation ops[1] = {Operation::kReset};  // To be filled.
    uint64_t key_hashes[1] = {0};            // To be filled.
    const char* keys[1] = {key};
    uint64_t key_lengths[1] = {key_length};
    const char* values[1] = {value};
    uint64_t value_lengths[1] = {value_length};
    Result* out_results[1] = {&out_result};
    char* out_values[1] = {value};
    size_t* out_value_lengths[1] = {&out_value_length};

    ::mica::processor::RequestArrayAccessor ra(
        1, ops, key_hashes, keys, key_lengths, values, value_lengths,
        out_results, out_values, out_value_lengths);

    const uint64_t batch_size = 1000000;
    for (uint64_t i = 0; i < prepopulation_key_count; i += batch_size) {
      printf("prepopulated %" PRIu64 " / %" PRIu64 " keys\n", i,
             prepopulation_key_count);
      fflush(stdout);

      uint64_t j_end = std::min(i + batch_size, prepopulation_key_count);
      for (uint64_t j = i; j < j_end; j++) {
        ops[0] = Operation::kSet;
        key_i = j;
        key_hashes[0] = hash(&key_i, sizeof(key_i));
        value_i = j;
        processor.process(ra);
        assert(out_result == Result::kSuccess);
      }
    }

    printf("prepopulated %" PRIu64 " / %" PRIu64 " keys\n",
           prepopulation_key_count, prepopulation_key_count);
    fflush(stdout);
  }

  DatagramServerConfig::Network network(config.get("network"));
  network.start();

  Server server(config.get("server"), &processor, &network, &dir_client);
  server.run();

  network.stop();

  return EXIT_SUCCESS;
}
