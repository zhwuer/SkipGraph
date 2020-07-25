/******************************************************************************\
 * This example is an implementation of the classical Dining Philosophers     *
 * exercise using only libcaf's event-based actor implementation.             *
\******************************************************************************/

#include <chrono>
#include <iostream>
#include <map>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

#include "caf/all.hpp"

CAF_BEGIN_TYPE_ID_BLOCK(dining_philosophers, first_custom_type_id)

CAF_ADD_ATOM(dining_philosophers, take_atom)
CAF_ADD_ATOM(dining_philosophers, taken_atom)
CAF_ADD_ATOM(dining_philosophers, eat_atom)
CAF_ADD_ATOM(dining_philosophers, think_atom)

CAF_END_TYPE_ID_BLOCK(dining_philosophers)

using std::cerr;
using std::cout;
using std::endl;
using std::chrono::seconds;

using namespace caf;

namespace {
    class philosopher : public event_based_actor {
    public:
        philosopher(actor_config& cfg, std::string n): event_based_actor(cfg), name_(std::move(n)) {
        }

        const char* name() const override {
            return name_.c_str();
        }

    protected:
        behavior make_behavior() override {
            return {
                    [=](think_atom) {
                        aout(this) << name_ << " starts to think\n";
                        become(thinking_);
                    },
            };
        }

    private:
        std::string name_;  // the name of this philosopher
        behavior thinking_; // initial behavior
    };

} // namespace

void caf_main(actor_system& system) {
    scoped_actor self{system};
    std::vector<std::string> names{"Plato", "Hume", "Kant", "Nietzsche", "Descartes"};
    for (size_t i = 0; i < 5; ++i) {
        auto tmp = self->spawn<philosopher>(names[i]);
        self->request(tmp, infinite, think_atom_v);
    }
}

CAF_MAIN(id_block::dining_philosophers)
