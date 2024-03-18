#include "./workload-smallbank.hpp"
#include "./hex.hpp"
#include <optional>
#include <glog/logging.h>
#include <fmt/core.h>

namespace spectrum {

Smallbank::Smallbank(size_t num_elements, double zipf_exponent): 
    evm_type{EVMType::STRAWMAN},
    rng{(zipf_exponent > 0.0 ? 
        std::unique_ptr<Random>(new Zipf(num_elements, zipf_exponent)) : 
        std::unique_ptr<Random>(new Unif(num_elements))
    )}
{
    LOG(INFO) << fmt::format("Smallbank({}, {})", num_elements, zipf_exponent);
    this->code = spectrum::from_hex(std::string{
        "608060405234801561001057600080fd5b506004361061007d5760003560e01c806397"
        "b632121161005b57806397b63212146100ea578063a5843f0814610106578063ad0f98"
        "c014610122578063bb27eb2c1461013e5761007d565b80631e01043914610082578063"
        "83406251146100b25780638ac10b9c146100ce575b600080fd5b61009c600480360381"
        "01906100979190610404565b61015a565b6040516100a99190610440565b6040518091"
        "0390f35b6100cc60048036038101906100c7919061045b565b61019f565b005b6100e8"
        "60048036038101906100e3919061049b565b6101e3565b005b61010460048036038101"
        "906100ff919061045b565b610274565b005b610120600480360381019061011b919061"
        "045b565b6102e4565b005b61013c6004803603810190610137919061045b565b610317"
        "565b005b6101586004803603810190610153919061045b565b610383565b005b600080"
        "6000808481526020019081526020016000205490506000600160008581526020019081"
        "526020016000205490508082610196919061051d565b92505050919050565b60008060"
        "00848152602001908152602001600020549050600082905080826101c7919061051d56"
        "5b6000808681526020019081526020016000208190555050505050565b600060016000"
        "8581526020019081526020016000205490506000600160008581526020019081526020"
        "01600020549050600083905080831061026c57808361022b9190610551565b92508082"
        "610239919061051d565b91508260016000888152602001908152602001600020819055"
        "508160016000878152602001908152602001600020819055505b505050505050565b60"
        "0080600084815260200190815260200160002054905060006001600084815260200190"
        "8152602001600020549050600060016000858152602001908152602001600020819055"
        "5080826102c8919061051d565b60008086815260200190815260200160002081905550"
        "50505050565b8060008084815260200190815260200160002081905550806001600084"
        "8152602001908152602001600020819055505050565b60006001600084815260200190"
        "815260200160002054905060008290508181116103635780826103479190610551565b"
        "600160008681526020019081526020016000208190555061037d565b60006001600086"
        "8152602001908152602001600020819055505b50505050565b60006001600084815260"
        "2001908152602001600020549050600082905080826103ac919061051d565b60016000"
        "8681526020019081526020016000208190555050505050565b600080fd5b6000819050"
        "919050565b6103e1816103ce565b81146103ec57600080fd5b50565b60008135905061"
        "03fe816103d8565b92915050565b60006020828403121561041a576104196103c9565b"
        "5b6000610428848285016103ef565b91505092915050565b61043a816103ce565b8252"
        "5050565b60006020820190506104556000830184610431565b92915050565b60008060"
        "408385031215610472576104716103c9565b5b6000610480858286016103ef565b9250"
        "506020610491858286016103ef565b9150509250929050565b60008060006060848603"
        "12156104b4576104b36103c9565b5b60006104c2868287016103ef565b935050602061"
        "04d3868287016103ef565b92505060406104e4868287016103ef565b91505092509250"
        "92565b7f4e487b71000000000000000000000000000000000000000000000000000000"
        "00600052601160045260246000fd5b6000610528826103ce565b9150610533836103ce"
        "565b925082820190508082111561054b5761054a6104ee565b5b92915050565b600061"
        "055c826103ce565b9150610567836103ce565b925082820390508181111561057f5761"
        "057e6104ee565b5b9291505056fea26469706673582212204ecbd2eaff1feae44da4ae"
        "ccb0e8e3055edb63a8145f1fea161d723fc8c5aa6f64736f6c63430008120033"
    }).value();
}

void Smallbank::SetEVMType(EVMType ty) {
    this->evm_type = ty;
}

inline std::string to_string(uint32_t key) {
    auto ss = std::ostringstream();
    ss << std::setw(64) << std::setfill('0') << key;
    return ss.str();
}

// Transaction Smallbank::Next() {
//     DLOG(INFO) << "smallbank next" << std::endl;
//     auto guard  = std::lock_guard{mu};
//     auto option = rng->Next() % 6;
//     #define X to_string(rng->Next())
//     auto input = spectrum::from_hex([&](){switch (option) {
//         case 0: return std::string{"1e010439"} + X;
//         case 1: return std::string{"bb27eb2c"} + X + X;
//         case 2: return std::string{"ad0f98c0"} + X + X;
//         case 3: return std::string{"83406251"} + X + X;
//         case 4: return std::string{"8ac10b9c"} + X + X + X;
//         case 5: return std::string{"97b63212"} + X + X;
//         default: throw "unreachable";
//     }}()).value();
//     #undef X
//     return Transaction(this->evm_type, evmc::address{0x1}, evmc::address{0x1}, std::span{code}, std::span{input});
// }

Transaction Smallbank::Next() {
    DLOG(INFO) << "smallbank next" << std::endl;
    auto guard  = std::lock_guard{mu};
    auto option = rng->Next() % 6;
    StaticPrediction p;
    #define X to_string(rng->Next())
    auto input = spectrum::from_hex([&](){switch (option) {
        case 0: {
            auto rnd1 = rng->Next();
            p.get.push_back(std::to_string(rnd1));
            return std::string{"1e010439"} + to_string(rnd1);
        }
        case 1: {
            auto rnd1 = rng->Next();
            auto rnd2 = rng->Next();
            p.put.push_back(std::to_string(rnd1));
            return std::string{"bb27eb2c"} + to_string(rnd1) + to_string(rnd2);
        }
        case 2: {
            auto rnd1 = rng->Next();
            auto rnd2 = rng->Next();
            p.put.push_back(std::to_string(rnd1));
            return std::string{"ad0f98c0"} + to_string(rnd1) + to_string(rnd2);
        }
        case 3: {
            auto rnd1 = rng->Next();
            auto rnd2 = rng->Next();
            p.put.push_back(std::to_string(rnd1));
            return std::string{"83406251"} + to_string(rnd1) + to_string(rnd2);
        }
        case 4: {
            auto rnd1 = rng->Next();
            auto rnd2 = rng->Next();
            auto rnd3 = rng->Next();
            p.put.push_back(std::to_string(rnd1));
            p.put.push_back(std::to_string(rnd2));
            return std::string{"8ac10b9c"} + to_string(rnd1) + to_string(rnd2) + to_string(rnd3);
        };
        case 5: {
            auto rnd1 = rng->Next();
            auto rnd2 = rng->Next();
            p.put.push_back(std::to_string(rnd1));
            p.put.push_back(std::to_string(rnd2));
            return std::string{"97b63212"} + to_string(rnd1) + to_string(rnd2);
        };
        default: throw "unreachable";
    }}()).value();
    #undef X
    auto txn = Transaction(this->evm_type, evmc::address{0x1}, evmc::address{0x1}, std::span{code}, std::span{input});
    txn.pred = p;
    return txn;
}

} // namespace spectrum
