/// Implements [`krabka_bdd::World`] for a scenario world whose `controller`
/// field holds an `Option<ControllerHandle>`: after every scenario, pass or
/// fail, the controller is shut down so its tasks do not outlive the test.
macro_rules! shuts_down_controller {
    ($world:ty) => {
        impl krabka_bdd::World for $world {
            async fn after(
                &mut self,
                _scenario: &krabka_bdd::ScenarioSpec,
                _outcome: &krabka_bdd::Outcome,
            ) {
                if let Some(controller) = self.controller.take() {
                    controller.shutdown().await;
                }
            }
        }
    };
}
