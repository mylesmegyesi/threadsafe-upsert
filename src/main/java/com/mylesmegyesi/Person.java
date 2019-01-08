package com.mylesmegyesi;

import java.time.Instant;

class Person {
  private final String email;
  private final String name;
  private final Instant createdAt;
  private final Instant updatedAt;

  Person(String email, String name, Instant createdAt, Instant updatedAt) {
    this.email = email;
    this.name = name;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
  }

  String getEmail() {
    return email;
  }

  String getName() {
    return name;
  }

  Instant getCreatedAt() {
    return createdAt;
  }

  Instant getUpdatedAt() {
    return updatedAt;
  }
}
