# To learn more about how to use Nix to configure your environment
# see: https://firebase.google.com/docs/studio/customize-workspace
{ pkgs, ... }: {
  # Which nixpkgs channel to use.
  channel = "stable-24.05"; # or "unstable"

  # Use https://search.nixos.org/packages to find packages
  packages = [
    # This creates a Python environment with the packages listed in requirements.txt
    (pkgs.python311.withPackages (ps: [
      ps.requests
      ps.google-cloud-storage
      ps.functions-framework
      ps.urllib3
      ps.certifi
      ps.google-cloud-secret-manager
    ]))
  ];

  # Sets environment variables in the workspace
  env = {
    # The GCP project ID is usually detected automatically by the gcloud CLI.
    # If not, you can set it explicitly:
    # GCP_PROJECT = "your-gcp-project-id";
    
    # Secret IDs for SP-API credentials
    SP_API_CLIENT_ID_SECRET_ID = "SP_API_CLIENT_ID";
    SP_API_CLIENT_SECRET_SECRET_ID = "SP_API_CLIENT_SECRET";
    SP_API_REFRESH_TOKEN_SECRET_ID = "SP_API_REFRESH_TOKEN";
  };

  idx = {
    # Search for the extensions you want on https://open-vsx.org/ and use "publisher.id"
    extensions = [
      "ms-python.python"
    ];

    # Enable previews
    previews = {
      enable = true;
    };

    # Workspace lifecycle hooks
    workspace = {
      # The onCreate hook is no longer necessary, as Nix now manages the Python packages directly.
      # Runs when a workspace is (re)started
      onStart = {
        # Example: start a background task to watch and re-build backend code
        # watch-backend = "npm run watch-backend";
      };
    };
  };
}
