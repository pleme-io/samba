{
  description = "samba — typed rate-limited consumer primitive for pleme-io";

  # substrate.rust.library dispatches over the committed Cargo.gen.lock (the
  # slim gen delta, reconstructed to a full BuildSpec in PURE NIX) rather than
  # through crate2nix.
  #
  # NOT `import "${substrate}/lib/rust-library.nix"`: that entry point routes
  # through crate2nix, which reads a derivation back during evaluation
  # (import-from-derivation) in two independent places -- its Cargo.nix generate
  # step, and `mkGitHash` (tools.nix:293) hashing a git dependency by building
  # a runCommand. Both are a DIFFERENT mechanism from the one substrate's
  # gen-pin bump removed, which lived in gen's git-source handling, so they
  # survived that fix untouched.
  inputs.substrate.url = "github:pleme-io/substrate";

  outputs =
    { substrate, ... }:
    (substrate.rust.library {
      src = ./.;
    })
    // {
      # `nix fmt` preserved across the switch. substrate.rust.library emits
      # {apps, checks, devShells, overlays, packages} and no formatter, and
      # substrate's own top-level `formatter` output is an EMPTY attrset
      # (measured, not assumed), so neither supplies one. Reading nixfmt-tree
      # out of substrate's OWN nixpkgs keeps the formatter working while adding
      # no input to this flake, and pins it to the same nixpkgs the build uses.
      #
      # The hand-written `overlays.default` this replaced is NOT lost:
      # substrate.rust.library already emits an overlay of exactly the same
      # shape, verified by applying it and reading back { samba }.
      formatter.aarch64-darwin =
        substrate.inputs.nixpkgs.legacyPackages.aarch64-darwin.nixfmt-tree;
    };
}
