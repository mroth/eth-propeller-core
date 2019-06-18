workflow "Test on push" {
  on = "push"
  resolves = ["Test"]
}

action "Test" {
  uses = "LucaFeger/action-maven-cli@master"
  args = "test"
}
