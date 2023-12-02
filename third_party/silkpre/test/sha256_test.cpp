/*
   Copyright 2022 The Silkpre Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <string>

#include <catch2/catch.hpp>

#include <silkpre/sha256.h>

#include "hex.hpp"

TEST_CASE("SHA256 of empty string") {
    uint8_t hash[32];
    silkpre_sha256(hash, nullptr, 0, /*use_cpu_extensions=*/false);
    CHECK(to_hex(hash, 32) == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

    silkpre_sha256(hash, nullptr, 0, /*use_cpu_extensions=*/true);
    CHECK(to_hex(hash, 32) == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST_CASE("SHA256 sample") {
    std::basic_string<uint8_t> input{
        from_hex("1234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234"
                 "567812345678")};

    uint8_t hash[32];
    silkpre_sha256(hash, input.data(), input.length(), /*use_cpu_extensions=*/false);
    CHECK(to_hex(hash, 32) == "7303caef875be8c39b2c2f1905ea24adcc024bef6830a965fe05370f3170dc52");

    silkpre_sha256(hash, input.data(), input.length(), /*use_cpu_extensions=*/true);
    CHECK(to_hex(hash, 32) == "7303caef875be8c39b2c2f1905ea24adcc024bef6830a965fe05370f3170dc52");
}
