{
  projectRootFile = ".git/config";

  programs.nixfmt = {
    enable = true;
  };

  programs.rustfmt = {
    enable = true;
  };
}
