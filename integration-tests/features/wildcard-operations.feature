Feature: Wildcard pattern support for stack operations
  Sceptre should support wildcard patterns for stack operations,
  allowing users to operate on multiple stacks matching a pattern

  Background:
    Given the sceptre project directory exists

  Scenario: Launch stacks with single-level wildcard
    Given stack "dev/vpc-1" does not exist
    And stack "dev/vpc-2" does not exist
    And stack "dev/app" does not exist
    And the template for stack "dev/vpc-1" is "valid_template.json"
    And the template for stack "dev/vpc-2" is "valid_template.json"
    And the template for stack "dev/app" is "valid_template.json"
    When the user launches stacks with wildcard pattern "dev/vpc-*.yaml"
    Then stack "dev/vpc-1" exists in "CREATE_COMPLETE" state
    And stack "dev/vpc-2" exists in "CREATE_COMPLETE" state
    And stack "dev/app" does not exist

  Scenario: Launch stacks with recursive wildcard pattern
    Given stack "dev/vpc" does not exist
    And stack "prod/vpc" does not exist
    And the template for stack "dev/vpc" is "valid_template.json"
    And the template for stack "prod/vpc" is "valid_template.json"
    When the user launches stacks with wildcard pattern "**/vpc.yaml"
    Then stack "dev/vpc" exists in "CREATE_COMPLETE" state
    And stack "prod/vpc" exists in "CREATE_COMPLETE" state

  Scenario: Create stacks with wildcard pattern
    Given stack "test/stack-1" does not exist
    And stack "test/stack-2" does not exist
    And the template for stack "test/stack-1" is "valid_template.json"
    And the template for stack "test/stack-2" is "valid_template.json"
    When the user creates stacks with wildcard pattern "test/stack-*.yaml"
    Then stack "test/stack-1" exists in "CREATE_COMPLETE" state
    And stack "test/stack-2" exists in "CREATE_COMPLETE" state

  Scenario: Update stacks with wildcard pattern
    Given stack "update-test/vpc" exists in "CREATE_COMPLETE" state
    And stack "update-test/app" exists in "CREATE_COMPLETE" state
    When the user updates stacks with wildcard pattern "update-test/*.yaml"
    Then stack "update-test/vpc" exists in "UPDATE_COMPLETE" state
    And stack "update-test/app" exists in "UPDATE_COMPLETE" state

  Scenario: Delete stacks with wildcard pattern
    Given stack "delete-test/vpc-1" exists in "CREATE_COMPLETE" state
    And stack "delete-test/vpc-2" exists in "CREATE_COMPLETE" state
    And stack "delete-test/app" exists in "CREATE_COMPLETE" state
    When the user deletes stacks with wildcard pattern "delete-test/vpc-*.yaml"
    Then stack "delete-test/vpc-1" does not exist
    And stack "delete-test/vpc-2" does not exist
    And stack "delete-test/app" exists in "CREATE_COMPLETE" state

  Scenario: Wildcard pattern matches no stacks fails
    When the user launches stacks with wildcard pattern "nonexistent/*.yaml"
    Then an error is raised containing "No stacks matched pattern"

  Scenario: Wildcard pattern with ignored stacks shows skip message
    Given stack "skip-test/vpc" does not exist
    And stack "skip-test/app" does not exist
    And the template for stack "skip-test/vpc" is "valid_template.json"
    And the template for stack "skip-test/app" is "valid_template.json"
    And stack config for "skip-test/app" has "ignore: true"
    When the user launches stacks with wildcard pattern "skip-test/*.yaml"
    Then stack "skip-test/vpc" exists in "CREATE_COMPLETE" state
    And stack "skip-test/app" does not exist

  Scenario: Create change-set with wildcard pattern
    Given stack "changeset-test/vpc" does not exist
    And stack "changeset-test/app" does not exist
    And the template for stack "changeset-test/vpc" is "valid_template.json"
    And the template for stack "changeset-test/app" is "valid_template.json"
    When the user creates change-set "test-changeset" for wildcard pattern "changeset-test/*.yaml"
    Then change-set "test-changeset" exists for stack "changeset-test/vpc"
    And change-set "test-changeset" exists for stack "changeset-test/app"

  Scenario: Wildcard pattern respects config inheritance
    Given stack "dev/us-east-1/vpc" does not exist
    And stack "prod/us-west-2/vpc" does not exist
    And the template for stack "dev/us-east-1/vpc" is "valid_template.json"
    And the template for stack "prod/us-west-2/vpc" is "valid_template.json"
    And stack group config "dev/config.yaml" sets "project_code: dev-project"
    And stack group config "prod/config.yaml" sets "project_code: prod-project"
    When the user launches stacks with wildcard pattern "**/vpc.yaml"
    Then stack "dev/us-east-1/vpc" has project_code "dev-project"
    And stack "prod/us-west-2/vpc" has project_code "prod-project"

  Scenario: Wildcard pattern with dependency resolution
    Given stack "dep-test/database" does not exist
    And stack "dep-test/app" does not exist
    And the template for stack "dep-test/database" is "valid_template.json"
    And the template for stack "dep-test/app" is "valid_template.json"
    And stack "dep-test/app" depends on stack "dep-test/database"
    When the user launches stacks with wildcard pattern "dep-test/app*.yaml"
    Then stack "dep-test/database" exists in "CREATE_COMPLETE" state
    And stack "dep-test/app" exists in "CREATE_COMPLETE" state
    And stack "dep-test/database" was created before "dep-test/app"

  Scenario: Wildcard pattern matching prefix
    Given stack "prefix-test/foobar" does not exist
    And stack "prefix-test/testbar" does not exist
    And stack "prefix-test/anothertestbar" does not exist
    And the template for stack "prefix-test/foobar" is "valid_template.json"
    And the template for stack "prefix-test/testbar" is "valid_template.json"
    And the template for stack "prefix-test/anothertestbar" is "valid_template.json"
    When the user launches stacks with wildcard pattern "prefix-test/*bar.yaml"
    Then stack "prefix-test/testbar" exists in "CREATE_COMPLETE" state
    And stack "prefix-test/anothertestbar" exists in "CREATE_COMPLETE" state
    And stack "prefix-test/foobar" does not exist

  Scenario: Wildcard pattern matching suffix
    Given stack "suffix-test/bar2" does not exist
    And stack "suffix-test/bartest" does not exist
    And stack "suffix-test/bar" does not exist
    And the template for stack "suffix-test/bar2" is "valid_template.json"
    And the template for stack "suffix-test/bartest" is "valid_template.json"
    And the template for stack "suffix-test/bar" is "valid_template.json"
    When the user launches stacks with wildcard pattern "suffix-test/bar*.yaml"
    Then stack "suffix-test/bar2" exists in "CREATE_COMPLETE" state
    And stack "suffix-test/bartest" exists in "CREATE_COMPLETE" state
    And stack "suffix-test/bar" exists in "CREATE_COMPLETE" state
