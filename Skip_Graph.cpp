#include <string>
#include <iostream>
#include <utility>
#include <vector>
#include <sstream>
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

constexpr auto task_timeout = std::chrono::seconds(10);

class config : public actor_system_config {
public:
    uint16_t port = 0;
    string host = "localhost";
    bool server_mode = false;

    config() {
        opt_group{custom_options_, "global"}
                .add(port, "port,p", "set port")
                .add(host, "host,H", "set host (ignored in server mode)")
                .add(server_mode, "server-mode,s", "enable server mode");
    }
};

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

namespace Node {

    struct state {
        int key{};
        int max_level{};
        uint16_t server_port{};
        string server_addr;
        bool delete_flag{};
        string ms_vector;
        vector<stateful_actor<Node::state>*> left_neighbor;
        vector<stateful_actor<Node::state>*> right_neighbor;
    };

    behavior operation_handler(stateful_actor<Node::state>* self) {
        return {
            [=](ok_atom) {
                auto target = actor_cast<stateful_actor<Node::state>*>(self->current_sender());
                cout << "The key is stored in " << target->state.server_addr + ":"
                    + std::to_string(target->state.server_port) << endl;
                return target;
            },
            [=](timeout_atom) {
                auto target = actor_cast<stateful_actor<Node::state>*>(self->current_sender());
                cout << "Could not find the key!" << endl;
                return target;
            },
            [=](get_atom, const strong_actor_ptr& start_actor, int search_key, int level) {
                stateful_actor<Node::state>* result;
                auto start_node = actor_cast<stateful_actor<Node::state>*>(start_actor);
                if (self->state.key == search_key) {
                    auto hdl = actor_cast<actor>(start_node);
                    self->request(hdl, task_timeout, ok_atom_v).then([&](const strong_actor_ptr& tmp) {
                        result = actor_cast<stateful_actor<Node::state>*>(tmp);
                    });
                } else if (self->state.key < search_key) {
                    while (level >= 0) {
                        if (self->state.right_neighbor[level]->state.key <= search_key) {
                            auto hdl = actor_cast<actor>(self->state.right_neighbor[level]);
                            self->request(hdl, task_timeout, get_atom_v, start_node, search_key, level).then([&](const strong_actor_ptr& tmp) {
                                result = actor_cast<stateful_actor<Node::state>*>(tmp);
                            });
                            break;
                        } else level = level - 1;
                    }
                } else {
                    while (level >= 0) {
                        if (self->state.left_neighbor[level]->state.key >= search_key) {
                            auto hdl = actor_cast<actor>(self->state.left_neighbor[level]);
                            self->request(hdl, task_timeout, get_atom_v, start_node, search_key, level).then([&](const strong_actor_ptr& tmp) {
                                result = actor_cast<stateful_actor<Node::state>*>(tmp);
                            });
                            break;
                        } else level = level - 1;
                    }
                }
                if (level < 0) {
                    auto hdl = actor_cast<actor>(start_node);
                    self->request(hdl, task_timeout, timeout_atom_v, self).then([&](const strong_actor_ptr& tmp) {
                        result = actor_cast<stateful_actor<Node::state>*>(tmp);
                    });
                }
                return result;
            },
            [=](join_atom) {
                auto new_node = actor_cast<stateful_actor<Node::state>*>(self->current_sender());
                // itself is the introduce node
                auto self_actor = actor_cast<actor>(self);
                stateful_actor<Node::state>* target_node;
                self->request(
                    self_actor, task_timeout, get_atom_v, self,
                    new_node->state.key, new_node->state.max_level
                ).then([&](const strong_actor_ptr& n) {
                    auto node = actor_cast<stateful_actor<Node::state>*>(n);
                    target_node = node;
                });
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
                    if (target_node != nullptr) {
                        l = l + 1;
                    } else return;
                }
            },
            [=](delete_atom) {
                for (int i = 0; i <= self->state.max_level; i++) {
                    self->state.left_neighbor[i]->state.right_neighbor[i] = self->state.right_neighbor[i];
                    self->state.right_neighbor[i]->state.left_neighbor[i] = self->state.left_neighbor[i];
                }
                self->state.delete_flag = true;
            },
            [=](put_atom, int k, int m, uint16_t p, string s, bool d, string ms) {
                self->state.key = k;
                self->state.max_level = m;
                self->state.server_port = p;
                self->state.server_addr = std::move(s);
                self->state.delete_flag = d;
                self->state.ms_vector = std::move(ms);
            }
        };
    }
}


using node = typed_actor<
        result<void>(ok_atom),
        result<void>(timeout_atom),
        result<void>(get_atom, strong_actor_ptr, int, int),
        result<void>(join_atom),
        result<void>(put_atom, int, int, uint16_t, string, bool, string)
>;

struct state {
    int key{};
    int max_level{};
    uint16_t server_port{};
    string server_addr;
    bool delete_flag{};
    string ms_vector;
    vector<node::stateful_pointer<state>> left_neighbor;
    vector<node::stateful_pointer<state>> right_neighbor;
};

node::behavior_type node_impl(node::stateful_pointer<state> self) {
    return {
        [=](ok_atom) {
            cout << "The key is stored in "
                 << self->state.server_addr + ":"
                 << std::to_string(self->state.server_port) << endl;
        },
        [=](timeout_atom) {
            cout << "Could not find the key!\n";
       },
        [=](get_atom, const strong_actor_ptr& start_actor, int search_key, int level) {
            string ss;
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
            node::stateful_pointer<state> target_node;
//            scoped_actor tmp{self->system()};
//            tmp->request(actor_cast<actor>(self), task_timeout, get_atom_v,
//                    actor_cast<strong_actor_ptr>(self), new_node->state.key, new_node->state.max_level);
            target_node = self;
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
                if (target_node) {
                    l = l + 1;
                } else break;
            }
        },
        [=](put_atom, int k, int m, uint16_t p, string s, bool d, string ms) {
            self->state.key = k;
            self->state.max_level = m;
            self->state.server_port = p;
            self->state.server_addr = std::move(s);
            self->state.delete_flag = d;
            self->state.ms_vector = std::move(ms);
            self->state.left_neighbor.resize(m);
            self->state.right_neighbor.resize(m);
        }
    };
}

struct serv_state {
    uint16_t port{};
    string addr;
    vector<node> nodes;
};

behavior server_handler(stateful_actor<serv_state>* self) {
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
            // Create new node && add the new node to the server node list
            auto new_actor = self->spawn(node_impl);
            auto new_node = actor_cast<node::stateful_pointer<state>>(new_actor);
            scoped_actor tmp{self->system()};
            // Initializing new actor
            tmp->request(new_actor, task_timeout, put_atom_v, key, intro_node->state.max_level, self->state.port,
                         self->state.addr, false, get_ms_vector(intro_node->state.max_level - 1)).receive(
                            [&](){cout << "New node initialized!!!" << endl;}, [&](error& err) {});
            self->state.nodes.insert(self->state.nodes.begin()+i, new_actor);
            // Send request to introduce node to add the new node
            // self->request(intro_actor, task_timeout, join_atom_v, actor_cast<strong_actor_ptr>(self->state.nodes.back()));
            new_node->request(intro_actor, task_timeout, join_atom_v);
            return "hello";
        },
        [=](put_atom, const string& host, uint16_t port) {
            // called in the run_server function
            self->state.addr = host;
            self->state.port = port;
            int max_level = 4;
            auto tmp = self->spawn(node_impl);
            anon_send(tmp, put_atom_v, INT32_MIN, max_level, port, host, false, get_ms_vector(max_level - 1));
            self->state.nodes.emplace_back(tmp);
            cout << "Successfully add two new nodes into server!" << endl;
        },
        [=](get_atom) {
            cout << "Show all nodes in server " << self->state.addr << ":" << self->state.port << endl;
            for (auto i : self->state.nodes) {
                cout << "Key = " << actor_cast<node::stateful_pointer<state>>(i)->state.key << endl;
            }
        }
    };
}

namespace Server {

    struct state {
        uint16_t port;
        string addr;
        // TODO optimize the data structure of node list
        vector<stateful_actor<Node::state>*> node_list; // sorted from smallest key to largest key
    };

    behavior server_handler(stateful_actor<Server::state>* self) {
        return {
            [=](get_atom, int key) {
                stringstream ss;
                cout << "Search node in server" << endl;
                // Find the start node
                stateful_actor<Node::state>* start_node;
                int i = 0;
                while (i < self->state.node_list.size() && self->state.node_list[i]->state.key < key) i++;
                start_node = self->state.node_list[i-1];
                self->request(actor_cast<actor>(start_node), task_timeout, ok_atom_v);
                self->request(actor_cast<actor>(start_node), task_timeout, get_atom_v, start_node, key, start_node->state.max_level);
                return ss.str();
            },
            [=](join_atom, int key) {
                stringstream ss;
                // Find the introduce node
                stateful_actor<Node::state>* intro_node;
                int i = 0;
                while (i < self->state.node_list.size() && self->state.node_list[i]->state.key < key) i++;
                intro_node = self->state.node_list[i-1];
                cout << intro_node->state.ms_vector << endl;
                // Create new node
                auto new_actor = self->spawn(Node::operation_handler);
                // add the new node to the server node list
                auto new_node = actor_cast<stateful_actor<Node::state>*>(new_actor);
                self->state.node_list.emplace_back(new_node);
                // Initialize new node
                scoped_actor tmp{self->system()};
                tmp->request(new_actor, task_timeout, put_atom_v, key, intro_node->state.max_level, self->state.port,
                             self->state.addr, false, get_ms_vector(intro_node->state.max_level - 1)).receive(
                            [](string& str){cout << str << endl;},
                            [&](error& err) {});
                // send request to introduce node to add the new node
                new_node->request(actor_cast<actor>(intro_node), task_timeout, join_atom_v);
                // new_node->request(actor_cast<actor>(intro_node), task_timeout, ok_atom_v);
                return ss.str();
            },
            [=](delete_atom, int key) {
                stringstream ss;
                ss << "Delete node from server" << endl;
                stateful_actor<Node::state>* leaving_node;
                self->request(actor_cast<actor>(self), task_timeout, get_atom_v, key).then(
                        [&](const strong_actor_ptr& ptr) {
                            auto result_node = actor_cast<stateful_actor<Node::state>*>(ptr);
                            leaving_node = result_node;
                        });
                self->request(actor_cast<actor>(leaving_node), task_timeout, delete_atom_v);
                return ss.str();
            },
            [=](put_atom, const string& host, uint16_t port) {
                // called in the run_server function
                self->state.addr = host;
                self->state.port = port;
                auto tmp = self->spawn(Node::operation_handler);
                int max_level = 4;
                // Initializing the first node
                anon_send(tmp, put_atom_v, INT32_MIN, max_level, port, host, false, get_ms_vector(max_level - 1));
                self->state.node_list.emplace_back(actor_cast<stateful_actor<Node::state>*>(tmp));
            },
            [=](get_atom) {
                stringstream ss;
                ss << "Show all nodes in server " << self->state.addr << ":" << self->state.port << endl;
                for (auto i : self->state.node_list) {
                    ss << "Key = " << i->state.key << endl;
                }
                return ss.str();
            }
        };
    }

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
            [=](get_atom op) {
                self->state.tasks.emplace_back(task{op, INT32_MIN});
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
                    aout(self) << R"(*** no server found at ")" << host << R"(":)" << port << endl;
                    return;
                }
                if (!ifs.empty()) {
                    aout(self) << R"(*** typed actor found at ")" << host << R"(":)"
                               << port << ", but expected an untyped actor " << endl;
                    return;
                }
                aout(self) << "*** successfully connected to server" << endl;
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
            if (key == INT32_MIN) {
                self->request(op_hdl, task_timeout, get_atom_v);
//                .then(
//                    [=](const string& str) { cout << str << endl; },
//                    [=](const error&) { self->send(self, op, key); }
//                );
            } else {
                self->request(op_hdl, task_timeout, op, key);
//                .then(
//                    [=](const string& str) { cout << str << endl; },
//                    [=](const error&) { self->send(self, op, key); }
//                );
            }
        };
        for (auto& x : self->state.tasks) {
            auto f = [&](auto op) { send_task(op, x.value); };
            caf::visit(f, x.op);
        }
        self->state.tasks.clear();
        return {
            [=](get_atom op, int key) { send_task(op, key); },
            [=](join_atom op, int key) { send_task(op, key); },
            [=](delete_atom op, int key) { send_task(op, key); },
            [=](get_atom op) { send_task(op, INT32_MIN); },
            [=](connect_atom, const string& host, uint16_t port) {
                connecting(self, host, port);
            },
        };
    }
}

void client_window(actor_system& system, const config& cfg) {
    auto usage = [] {
        cout << "Usage:" << endl
             << "  quit                  : terminates the program" << endl
             << "  connect <host> <port> : connects to a remote actor" << endl
             << "  search <key>          : search a node in Skip Graph" << endl
             << "  add <key>             : add a new node in Skip Graph" << endl
             << "  delete <key>          : delete a exist node in Skip Graph" << endl
             << "  show                  : display the nodes in this server" << endl
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
            if (cmd != "quit" && cmd != "show") {
                return;
            } else if (cmd == "quit") {
                anon_send_exit(client, exit_reason::user_shutdown);
                done = true;
            } else if (cmd == "show") {
                anon_send(client, get_atom_v);
            }
        },
        [&](string& arg0, string& arg1, string& arg2) {
            if (arg0 == "connect") {
                char* end = nullptr;
                auto local_port = strtoul(arg2.c_str(), &end, 10);
                if (end != arg2.c_str() + arg2.size())
                    cout << R"(")" << arg2 << R"(" is not an unsigned integer)" << endl;
                else if (local_port > std::numeric_limits<uint16_t>::max())
                    cout << R"(")" << arg2 << R"(" > )"
                         << std::numeric_limits<uint16_t>::max() << endl;
                else
                    anon_send(client, connect_atom_v, move(arg1),
                              static_cast<uint16_t>(local_port));
            } else {
                cout << "error command!!!" << endl;
                usage();
            }
        },
        [&](const string& arg0, const string& arg1) {
            auto key = toint(arg1);
            if (key) {
                if (arg0 == "search") anon_send(client, get_atom_v, *key);
                else if (arg0 == "add") anon_send(client, join_atom_v, *key);
                else if (arg0 == "delete") anon_send(client, delete_atom_v, *key);
                else cout << "error command!!!" << endl;
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

void caf_main(actor_system& system, const config& cfg) {
    auto f = cfg.server_mode ? run_server : client_window;
    f(system, cfg);
}

// creates a main function for us that calls our caf_main
CAF_MAIN(io::middleman)
