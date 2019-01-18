package com.mylesmegyesi;

import java.time.Instant;

class Person {
  final String email;
  final String name;
  final Instant createdAt;
  final Instant updatedAt;

  Person(String email, String name, Instant createdAt, Instant updatedAt) {
    this.email = email;
    this.name = name;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
  }
}
