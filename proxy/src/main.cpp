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

// Global server pointer for signal handlers
static proxy::ProxyServer* g_proxy_server = nullptr;

static void sigusr2Handler(int /*sig*/) {
    // Trigger hot upgrade from signal handler context
    // We just set a flag; the actual upgrade is handled in main loop
    if (g_proxy_server) {
        g_proxy_server->triggerHotUpgrade();
    }
}

static void printUsage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  -c, --config <file>    Configuration file path (default: proxy.yaml)\n"
              << "  -w, --workers <num>    Number of worker threads (0 = auto)\n"
              << "  -a, --admin-port <p>   Admin management port\n"
              << "  -l, --log-level <lvl>  Log level: DEBUG, INFO, WARN, ERROR\n"
              << "  -f, --log-file <file>  Log file path (empty = stdout)\n"
              << "  -p, --password <pwd>   Proxy authentication password\n"
              << "  --upgrade-from <path>  Receive fds from old process via UDS (internal use)\n"
              << "  -h, --help             Show this help message\n";
}

static std::string getConfigPath(int argc, char* argv[]) {
    static struct option long_options[] = {
        {"config", required_argument, nullptr, 'c'},
        {"upgrade-from", required_argument, nullptr, 'U'},
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

// Parse --upgrade-from argument
static std::string getUpgradeSocketPath(int argc, char* argv[]) {
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg.rfind("--upgrade-from=", 0) == 0) {
            return arg.substr(strlen("--upgrade-from="));
        }
        if (arg == "--upgrade-from" && i + 1 < argc) {
            return argv[i + 1];
        }
    }
    return "";
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

    // Check for upgrade-from argument
    std::string upgrade_socket = getUpgradeSocketPath(argc, argv);

    // Step 2: Load configuration
    proxy::Config config;
    if (!config.loadFromFile(config_path)) {
        std::cerr << "Warning: Could not load config file '" << config_path
                  << "', using defaults." << std::endl;
    }

    // Step 3: Apply command line overrides
    config.applyCommandLine(argc, argv);
    config.setConfigPath(config_path);

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
    g_proxy_server = &server;

    if (!server.init()) {
        LOG_ERROR("Failed to initialize proxy server");
        return 1;
    }

    // Register SIGUSR2 for hot upgrade
    struct sigaction sa_usr2{};
    sa_usr2.sa_handler = sigusr2Handler;
    sigemptyset(&sa_usr2.sa_mask);
    sa_usr2.sa_flags = 0;
    sigaction(SIGUSR2, &sa_usr2, nullptr);

    // If upgrading from old process, pre-start workers then receive fds
    // so that dispatched client fds are immediately processed
    if (!upgrade_socket.empty()) {
        LOG_INFO("Upgrade mode: pre-starting workers before receiving fds");
        server.startWorkers();

        LOG_INFO("Upgrade mode: receiving from old process via '%s'", upgrade_socket.c_str());
        auto& upgrade_mgr = server.getHotUpgradeManager();
        if (!upgrade_mgr.receiveFromOldProcess(upgrade_socket)) {
            LOG_ERROR("Failed to receive state from old process, starting fresh");
        }
    }

    // Step 6: Run the server (blocks until shutdown)
    server.run();

    g_proxy_server = nullptr;
    LOG_INFO("Redis MUX Proxy exited.");
    return 0;
}