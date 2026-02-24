#include "config.h"
#include "logger.h"
#include "proxy_server.h"
#include "client_connection.h"

#include <iostream>
#include <csignal>
#include <cstring>
#include <getopt.h>
#include <thread>
#include <unistd.h>

static void printUsage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  -c, --config <file>    Configuration file path (default: proxy.yaml)\n"
              << "  -w, --workers <num>    Number of worker threads (0 = auto)\n"
              << "  -a, --admin-port <p>   Admin management port\n"
              << "  -l, --log-level <lvl>  Log level: DEBUG, INFO, WARN, ERROR\n"
              << "  -f, --log-file <file>  Log file path (empty = stdout)\n"
              << "  -p, --password <pwd>   Proxy authentication password\n"
              << "  -h, --help             Show this help message\n";
}

static std::string getConfigPath(int argc, char* argv[]) {
    static struct option long_options[] = {
        {"config", required_argument, nullptr, 'c'},
        {nullptr, 0, nullptr, 0}
    };

    optind = 1;
    int opt;
    while ((opt = getopt_long(argc, argv, "c:w:a:l:f:p:h", long_options, nullptr)) != -1) {
        if (opt == 'c') return optarg;
        if (opt == 'h') {
            printUsage(argv[0]);
            exit(0);
        }
    }
    return "proxy.yaml";
}

int main(int argc, char* argv[]) {
    // Check for --help first
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printUsage(argv[0]);
            return 0;
        }
    }

    // Step 1: Get config file path
    std::string config_path = getConfigPath(argc, argv);

    // Step 2: Load configuration
    proxy::Config config;
    if (!config.loadFromFile(config_path)) {
        std::cerr << "Warning: Could not load config file '" << config_path
                  << "', using defaults." << std::endl;
    }

    // Step 3: Apply command line overrides
    config.applyCommandLine(argc, argv);

    // Step 4: Initialize logger
    const auto& log_cfg = config.getLogConfig();
    if (!proxy::Logger::instance().init(log_cfg.file, log_cfg.level,
                                         log_cfg.max_size_mb, log_cfg.max_files)) {
        std::cerr << "Warning: Logger initialization failed, using stdout." << std::endl;
    }

    LOG_INFO("Redis MUX Proxy starting... (pid=%d)", getpid());

    // Set global proxy password for client connections
    proxy::setGlobalProxyPassword(&config.getProxyPassword());

    // Step 5: Create and initialize ProxyServer
    proxy::ProxyServer server(config);
    if (!server.init()) {
        LOG_ERROR("Failed to initialize proxy server");
        return 1;
    }

    // Step 6: Run the server (blocks until shutdown)
    server.run();

    LOG_INFO("Redis MUX Proxy exited.");
    return 0;
}