{
  description = "A parallel and robust Vulkan Conformance Test Suite runner";

  inputs = {
    naersk.url = "github:nmattia/naersk";
  };

  outputs = { self, naersk, nixpkgs }:
    let
      forAllSystems = nixpkgs.lib.genAttrs [ "x86_64-linux" "x86_64-darwin" "i686-linux" "aarch64-linux" ];
    in
      {
        packages = forAllSystems (
          system: {
            deqp-runner = naersk.lib."${system}".buildPackage {
              pname = "deqp-runner";
              root = ./.;
            };
          }
        );
        defaultPackage = forAllSystems (system: self.packages."${system}".deqp-runner);
      };
}
