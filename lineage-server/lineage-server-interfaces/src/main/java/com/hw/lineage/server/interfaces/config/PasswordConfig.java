/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hw.lineage.server.interfaces.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * @description: PasswordConfig
 * @author: HamaWhite
 */
@Configuration
public class PasswordConfig {

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new LegacyAwareBCryptPasswordEncoder();
    }

    private static class LegacyAwareBCryptPasswordEncoder implements PasswordEncoder {

        private static final String BCRYPT_PREFIX = "{bcrypt}";

        private final BCryptPasswordEncoder delegate = new BCryptPasswordEncoder();

        @Override
        public String encode(CharSequence rawPassword) {
            return delegate.encode(rawPassword);
        }

        @Override
        public boolean matches(CharSequence rawPassword, String encodedPassword) {
            if (rawPassword == null || encodedPassword == null) {
                return false;
            }
            if (encodedPassword.startsWith(BCRYPT_PREFIX)) {
                return delegate.matches(rawPassword, encodedPassword.substring(BCRYPT_PREFIX.length()));
            }
            if (encodedPassword.startsWith("$2a$")
                    || encodedPassword.startsWith("$2b$")
                    || encodedPassword.startsWith("$2y$")) {
                return delegate.matches(rawPassword, encodedPassword);
            }
            return rawPassword.toString().equals(encodedPassword);
        }
    }
}
