#include <string>
#include <iostream>
#include <utility>
#include <vector>
#include <chrono>
#include <random>
#include "caf/all.hpp"
#include "caf/io/all.hpp"

using std::cout;
using std::endl;
using std::string;
using std::vector;
using std::stringstream;
using namespace caf;

using node = typed_actor<
        result<void>(ok_atom),
        result<void>(timeout_atom),
        result<void>(get_atom, strong_actor_ptr, int, int),
        result<void>(join_atom),
        result<void>(delete_atom, int),
        result<void>(put_atom, int, int, uint16_t, string, bool, string),
        result<void>(get_atom)
>;

CAF_BEGIN_TYPE_ID_BLOCK(skip_graph, first_custom_type_id)
    CAF_ADD_TYPE_ID(skip_graph, (node))
CAF_END_TYPE_ID_BLOCK(skip_graph)

constexpr auto task_timeout = std::chrono::seconds(10);

string trim(string s) {
    auto not_space = [](char c) { return isspace(c) == 0; };
    s.erase(s.begin(), find_if(s.begin(), s.end(), not_space));
    s.erase(find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
    return s;
}

optional<int> toint(const string& str) {
    char* end;
    auto result = static_cast<int>(strtol(str.c_str(), &end, 10));
    if (end == str.c_str() + str.size())
        return result;
    return none;
}

string get_ms_vector(int length) {
    string res;
    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine e(seed);
    while (length--) res += (e() % 2) ? "1" : "0";
    return res;
}

struct address {
    string ip;
    uint16_t port{};
};

struct state {
    int key{};
    int max_level{};
    address server;
    bool delete_flag{};
    string ms_vector;
    vector<node::stateful_pointer<state>> left_neighbor;
    vector<node::stateful_pointer<state>> right_neighbor;
};

node::behavior_type node_impl(node::stateful_pointer<state> self) {
    return {
        [=](ok_atom) {
            cout << "The key is stored in "
                 << self->state.server.ip + ":"
                 << std::to_string(self->state.server.port) << endl;
        },
        [=](timeout_atom) {
            cout << "Could not find the key!\n";
        },
        [=](get_atom, const strong_actor_ptr& start_actor, int search_key, int level) {
            scoped_actor tmp{self->system()};
            // auto start_node = actor_cast<node::stateful_pointer<state>>(start_actor);
            if (self->state.key == search_key) {
                auto hdl = actor_cast<actor>(start_actor);
                self->request(hdl, task_timeout, ok_atom_v);
            } else if (self->state.key < search_key) {
                while (level >= 0) {
                    if (self->state.right_neighbor[level] && self->state.right_neighbor[level]->state.key <= search_key) {
                        auto hdl = actor_cast<actor>(self->state.right_neighbor[level]);
                        self->request(hdl, task_timeout, get_atom_v, start_actor, search_key, level);
                        break;
                    } else level = level - 1;
                }
            } else {
                while (level >= 0) {
                    if (self->state.left_neighbor[level] && self->state.left_neighbor[level]->state.key >= search_key) {
                        auto hdl = actor_cast<actor>(self->state.left_neighbor[level]);
                        self->request(hdl, task_timeout, get_atom_v, start_actor, search_key, level);
                        break;
                    } else level = level - 1;
                }
            }
            if (level < 0) {
                auto hdl = actor_cast<actor>(start_actor);
                self->request(hdl, task_timeout, timeout_atom_v);
            }
        },
        [=](join_atom) {
            auto new_node = actor_cast<node::stateful_pointer<state>>(self->current_sender());
            cout << "New_node: " << new_node->address().get() << " Key: " << new_node->state.key << endl;
            // itself is the introduce node
            node::stateful_pointer<state> target_node = self;
            while (target_node->state.right_neighbor[0] && target_node->state.right_neighbor[0]->state.key < new_node->state.key)
                target_node = target_node->state.right_neighbor[0];
            int l = 0;
            while (true) {
                // insert new node after target node
                new_node->state.left_neighbor[l] = target_node;
                new_node->state.right_neighbor[l] = target_node->state.right_neighbor[l];
                if (target_node->state.right_neighbor[l]) {
                    target_node->state.right_neighbor[l]->state.left_neighbor[l] = new_node;
                }
                target_node->state.right_neighbor[l] = new_node;

                string t_ms_vector = new_node->state.ms_vector.substr(0, l+1); // target membership vector
                while (target_node != nullptr && target_node->state.ms_vector.substr(0, l+1) != t_ms_vector) {
                    target_node = target_node->state.left_neighbor[l];
                }
                if (target_node && l < new_node->state.max_level) {
                    l = l + 1;
                } else break;
            }
        },
        [=](delete_atom, int key) {
            node::stateful_pointer<state> target_node = self;
            while (target_node && target_node->state.key != key)
                target_node = target_node->state.right_neighbor[0];
            if (target_node == nullptr || target_node->state.key != key) {
                cout << "The element does not exist!" << endl;
                return;
            } else {
                for (int i = 0; i <= target_node->state.max_level; i++) {
                    if (target_node->state.left_neighbor[i])
                        target_node->state.left_neighbor[i]->state.right_neighbor[i] = target_node->state.right_neighbor[i];
                    if (target_node->state.right_neighbor[i])
                        target_node->state.right_neighbor[i]->state.left_neighbor[i] = target_node->state.left_neighbor[i];
                }
                target_node->state.delete_flag = true;
                cout << "Successfully delete the node " << std::to_string(key) << endl;
            }
        },
        [=](put_atom, int k, int m, uint16_t p, string s, bool d, string ms) {
            self->state.key = k;
            self->state.max_level = m;
            self->state.server.port = p;
            self->state.server.ip = std::move(s);
            self->state.delete_flag = d;
            self->state.ms_vector = std::move(ms);
            self->state.left_neighbor.resize(m);
            self->state.right_neighbor.resize(m);
        },
        [=](get_atom){
            cout << "Show all nodes:" << endl;
            node::stateful_pointer<state> begin_node = self;
            while (begin_node->state.left_neighbor[0])
                begin_node = begin_node->state.right_neighbor[0];
            int max_level = begin_node->state.max_level;
            for (int i = max_level-1; i >= 0; i--) {
                auto iter = begin_node;
                cout << "HEAD" << " (" << iter->state.ms_vector << ") ----- ";
                while (iter->state.right_neighbor[i]) {
                    cout << iter->state.right_neighbor[i]->state.key
                         << " (" << iter->state.right_neighbor[i]->state.ms_vector << ") ----- ";
                    iter = iter->state.right_neighbor[i];
                }
                cout << "TAIL" << endl;
            }

        }
    };
}

struct server_state {
    address myaddr;
    vector<node> nodes;
    vector<address> servers;
};

behavior server_handler(stateful_actor<server_state>* self) {
    return {
        [=](get_atom, int key) {
            // Find the start node
            int i = 0;
            while (i < self->state.nodes.size() && actor_cast<node::stateful_pointer<state>>(self->state.nodes[i])->state.key < key) i++;
            auto start_node = self->state.nodes[i-1];
            int level = actor_cast<node::stateful_pointer<state>>(start_node)->state.max_level;
            // Send request to start node to search
            self->request(actor_cast<actor>(start_node), task_timeout, get_atom_v, actor_cast<strong_actor_ptr>(start_node), key, level);
       },
        [=](join_atom, int key) {
            int i = 0;
            while (i < self->state.nodes.size() && actor_cast<node::stateful_pointer<state>>(self->state.nodes[i])->state.key < key) i++;
            // Find the introduce node
            auto intro_actor = self->state.nodes[i-1];
            auto intro_node = actor_cast<node::stateful_pointer<state>>(intro_actor);
            scoped_actor tmp{self->system()};

            // Find the target server TODO: Load balance optimization
            address another_server{string(""), 0};
            if (!self->state.servers.empty()) another_server = self->state.servers.back();
            auto conn = self->system().middleman().connect(another_server.ip, another_server.port);

            if (!conn) {  // if there are no other server nodes
                // Create new node && add the new node to the server node list
                auto new_actor = self->spawn(node_impl);
                auto new_node = actor_cast<node::stateful_pointer<state>>(new_actor);
                // Initializing new actor
                tmp->request(new_actor, task_timeout, put_atom_v, key, intro_node->state.max_level,
                             self->state.myaddr.port, self->state.myaddr.ip, false,
                             get_ms_vector(intro_node->state.max_level - 1)).receive([](){}, [&](error& err) {});
                self->state.nodes.insert(self->state.nodes.begin()+i, new_actor);
                // Send request to introduce node to add the new node
                new_node->request(intro_actor, task_timeout, join_atom_v);
            } else {    // find another server to store the data node --- load balance
                auto mm = self->system().middleman().actor_handle();
                self->request(mm, infinite, connect_atom_v, another_server.ip, another_server.port).await(
                    [=](const node_id&, strong_actor_ptr serv, const std::set<string>& ifs) {
                        if (!serv) {
                            aout(self) << R"(*** no server found at ")" << another_server.ip << R"(":)"
                                       << another_server.port << endl;
                            return;
                        }
                        if (!ifs.empty()) {
                            aout(self) << R"(*** typed actor found at ")" << another_server.ip << R"(":)"
                                       << another_server.port << ", but expected an untyped actor " << endl;
                            return;
                        }
                        auto remote_server = actor_cast<actor>(serv);
                        self->request(remote_server, task_timeout, join_atom_v, key);
                    },
                    [=](const error& err) {
                        aout(self) << R"(*** cannot connect to ")" << another_server.ip << R"(":)" << another_server.port
                                   << " => " << to_string(err) << endl;
                    }
                );
            }

        },
        [=](delete_atom, int key) {
            int i = 0;
            while (i < self->state.nodes.size() && actor_cast<node::stateful_pointer<state>>(self->state.nodes[i])->state.key <= key) i++;
            auto leaving_node = self->state.nodes[i-1];
            if (actor_cast<node::stateful_pointer<state>>(leaving_node)->state.key == key)
                self->state.nodes.erase(self->state.nodes.begin()+i-1);
            self->request(actor_cast<actor>(leaving_node), task_timeout, delete_atom_v, key);
        },
        [=](put_atom, const string& host, uint16_t port) {
            // called in the run_server function
            self->state.myaddr.ip = host;
            self->state.myaddr.port = port;
            int max_level = 4;
            auto tmp = self->spawn(node_impl);
            anon_send(tmp, put_atom_v, INT32_MIN, max_level, port, host, false, get_ms_vector(max_level - 1));
            self->state.nodes.emplace_back(tmp);
        },
        [=](get_atom, const string& str) {
            if (str == "server") {
                cout << "Show nodes in server " << self->state.myaddr.ip << ":" << self->state.myaddr.port << endl;
                for (auto i : self->state.nodes) {
                    cout << "Key = " << actor_cast<node::stateful_pointer<state>>(i)->state.key << endl;
                }
            } else {
                self->request(actor_cast<actor>(self->state.nodes[0]), task_timeout, get_atom_v);
            }
        },
        [=](add_atom, const string& host, uint16_t port){
            if (host != self->state.myaddr.ip) {
                self->state.servers.push_back({host, port});
                cout << "Successfully added a remote server: " << host << ":" << std::to_string(port) << endl;
            }
        }
    };
}

namespace Client {

    struct task {
        caf::variant<get_atom, join_atom, delete_atom> op;
        int value;
    };

    struct state {
        strong_actor_ptr cur_server;
        vector<task> tasks;
    };

    using client_node = stateful_actor<Client::state>*;
    behavior unconnected(client_node);
    void connecting(client_node, const string& host, uint16_t port);
    behavior running(client_node, const actor& op_hdl);

    behavior init(client_node self) {
        self->set_down_handler([=](const down_msg& dm) {
            if (dm.source == self->state.cur_server) {
                aout(self) << "*** lost connection to server" << endl;
                self->state.cur_server = nullptr;
                self->become(unconnected(self));
            }
        });
        return unconnected(self);
    }

    behavior unconnected(client_node self) {
        return {
            [=](get_atom op, int key) {
                self->state.tasks.emplace_back(task{op, key});
            },
            [=](join_atom op, int key) {
                self->state.tasks.emplace_back(task{op, key});
            },
            [=](delete_atom op, int key) {
                self->state.tasks.emplace_back(task{op, key});
            },
            [=](get_atom op, const string& str) {
                if (str == "server") self->state.tasks.emplace_back(task{op, INT32_MIN});
                else self->state.tasks.emplace_back(task{op, INT32_MAX});
            },
            [=](add_atom, const string& host, uint16_t port) {
                cout << "Please connect to a server first" << endl;
            },
            [=](connect_atom, const string& host, uint16_t port) {
                connecting(self, host, port);
            }
        };
    }

    void connecting(client_node self, const string& host, uint16_t port) {
        self->state.cur_server = nullptr;
        auto mm = self->system().middleman().actor_handle();
        self->request(mm, infinite, connect_atom_v, host, port).await(
            [=](const node_id&, strong_actor_ptr serv, const std::set<string>& ifs) {
                if (!serv) {
                    cout << R"(*** no server found at ")" << host << R"(":)" << port << endl;
                    return;
                }
                if (!ifs.empty()) {
                    cout << R"(*** typed actor found at ")" << host << R"(":)"
                               << port << ", but expected an untyped actor " << endl;
                    return;
                }
                cout << "*** successfully connected to server" << endl;
                self->state.cur_server = serv;
                auto hdl = actor_cast<actor>(serv);
                self->monitor(hdl);
                self->become(running(self, hdl));
            },
            [=](const error& err) {
                aout(self) << R"(*** cannot connect to ")" << host << R"(":)" << port
                           << " => " << to_string(err) << endl;
                self->become(unconnected(self));
            }
        );
    }

    behavior running(client_node self, const actor& op_hdl) {
        auto send_task = [=](auto op, int key) {
            self->request(op_hdl, task_timeout, op, key);
//                .then(
//                    [=](const string& str) { cout << str << endl; },
//                    [=](const error&) { self->send(self, op, key); }
//                );
        };
        auto show_task = [=](auto op, const string& str) {
            self->request(op_hdl, task_timeout, get_atom_v, str);
        };
        for (auto& x : self->state.tasks) {
            auto f = [&](auto op) {
                if (x.value == INT32_MIN) show_task(op, "server");
                else if (x.value == INT32_MAX) show_task(op, "all");
                else send_task(op, x.value);
            };
            caf::visit(f, x.op);
        }
        self->state.tasks.clear();
        return {
            [=](get_atom op, int key) { send_task(op, key); },
            [=](join_atom op, int key) { send_task(op, key); },
            [=](delete_atom op, int key) { send_task(op, key); },
            [=](get_atom op, const string& str) { show_task(op, str); },
            [=](add_atom, const string& host, uint16_t port) {
                self->request(op_hdl, task_timeout, add_atom_v, host, port);
            },
            [=](connect_atom, const string& host, uint16_t port) {
                connecting(self, host, port);
            },
        };
    }
}

class config : public actor_system_config {
public:
    uint16_t port = 0;
    string host = "localhost";
    bool server_mode = false;

    config() {
        add_actor_type("node_implementation", node_impl);
        opt_group{custom_options_, "global"}
                .add(port, "port,p", "set port")
                .add(host, "host,H", "set host (ignored in server mode)")
                .add(server_mode, "server-mode,s", "enable server mode");
    }
};

void client_window(actor_system& system, const config& cfg) {
    auto usage = [] {
        cout << "Usage:" << endl
             << "  quit                  : terminates the program" << endl
             << "  connect <host> <port> : connects to a remote server" << endl
             << "  add <host> <port>     : add a remote server" << endl
             << "  search <key>          : search a node in Skip Graph" << endl
             << "  add <key>             : add a new node in Skip Graph" << endl
             << "  delete <key>          : delete a exist node in Skip Graph" << endl
             << "  show <server>/<all>   : display the nodes in this server or in all server" << endl
             << endl;
    };
    usage();
    bool done = false;
    auto client = system.spawn(Client::init);
    if (!cfg.host.empty() && cfg.port > 0)
        anon_send(client, connect_atom_v, cfg.host, cfg.port);
    else
        cout << "*** no server received via config, "
             << R"(please use "connect <host> <port>" before using the calculator)"
             << endl;
    message_handler eval{
        [&](const string& cmd) {
            if (cmd != "quit") {
                cout << "error command!!!" << endl;
                usage();
                return;
            }
            anon_send_exit(client, exit_reason::user_shutdown);
            done = true;
        },
        [&](string& arg0, string& arg1, string& arg2) {
            if (arg0 == "connect" || arg0 == "add") {
                char* end = nullptr;
                auto local_port = strtoul(arg2.c_str(), &end, 10);
                if (end != arg2.c_str() + arg2.size())
                    cout << R"(")" << arg2 << R"(" is not an unsigned integer)" << endl;
                else if (local_port > std::numeric_limits<uint16_t>::max())
                    cout << R"(")" << arg2 << R"(" > )"
                         << std::numeric_limits<uint16_t>::max() << endl;
                else {
                    if (arg0 == "connect")
                        anon_send(client, connect_atom_v, move(arg1), static_cast<uint16_t>(local_port));
                    else
                        anon_send(client, add_atom_v, move(arg1), static_cast<uint16_t>(local_port));
                }
            } else {
                cout << "error command!!!" << endl;
                usage();
            }
        },
        [&](const string& arg0, const string& arg1) {
            if (arg0 == "show") {
                anon_send(client, get_atom_v, arg1);
            } else {
                auto key = toint(arg1);
                if (key) {
                    if (arg0 == "search") anon_send(client, get_atom_v, *key);
                    else if (arg0 == "add") anon_send(client, join_atom_v, *key);
                    else if (arg0 == "delete") anon_send(client, delete_atom_v, *key);
                    else cout << "error command!!!" << endl;
                }
            }

        }
    };

    string line;
    while (!done && std::getline(std::cin, line)) {
        line = trim(std::move(line));
        vector<string> words;
        split(words, line, is_any_of(" "), token_compress_on);
        auto msg = message_builder(words.begin(), words.end()).move_to_message();
        if (!eval(msg))
            usage();
        cout << ">>> ";
    }
}

void run_server(actor_system& system, const config& cfg) {
    auto op_hdl = system.spawn(server_handler);
    cout << "*** try publish at port " << cfg.port << endl;
    auto expected_port = io::publish(op_hdl, cfg.port, nullptr, true);
    if (!expected_port) {
        std::cerr << "*** publish failed: " << to_string(expected_port.error()) << endl;
        return;
    }
    cout << "*** server successfully published at port " << *expected_port << endl
         << "*** press [enter] to quit" << endl;
    anon_send(op_hdl, put_atom_v, cfg.host, *expected_port);
    string dummy;
    std::getline(std::cin, dummy);
    cout << "Good Bye!" << endl;
    anon_send_exit(op_hdl, exit_reason::user_shutdown);
}

[[maybe_unused]] void caf_main(actor_system& system, const config& cfg) {
    auto f = cfg.server_mode ? run_server : client_window;
    f(system, cfg);
}

// creates a main function for us that calls our caf_main
CAF_MAIN(id_block::skip_graph, io::middleman)
