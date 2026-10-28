Feature: src coordinator unified migration upgrade

  Scenario: tests: conversion refuses what kafka cannot carry over
    Then coordinator unified migration upgrade tests: conversion refuses what kafka cannot carry over

  Scenario: tests: subscription versions decode as kafka reads them
    Then coordinator unified migration upgrade tests: subscription versions decode as kafka reads them

  Scenario: tests: validate online upgrade follows kafka
    Then coordinator unified migration upgrade tests: validate online upgrade follows kafka
