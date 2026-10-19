# podman's containers/image library requires a signature-verification policy.json
# for `podman load` (used by `just build`) — a check that `podman info` does NOT
# perform. The NixOS `virtualisation.containers` module normally installs
# /etc/containers/policy.json, but a host that gets podman purely from the flake
# dev-shell never gets it, so `just build` fails at `podman load` even though
# `podman info` is green. This podman build exposes no `--signature-policy` flag
# and no env override, so the file must exist at one of the two lookup paths.
# Ensure the user-level default (idempotent; never overwrites a system or user one).
system_policy="/etc/containers/policy.json"
user_policy="${HOME}/.config/containers/policy.json"
if [ -f "$system_policy" ] || [ -f "$user_policy" ]; then
    echo "Containers signature policy present (podman load can run)"
elif mkdir -p "$(dirname "$user_policy")" 2>/dev/null &&
    printf '{ "default": [ { "type": "insecureAcceptAnything" } ] }\n' >"$user_policy"; then
    echo "Wrote a permissive containers policy to $user_policy (needed by podman load)"
else
    log_warning "No containers policy.json and could not create $user_policy."
    echo "Create it manually: printf '{ \"default\": [ { \"type\": \"insecureAcceptAnything\" } ] }\\n' > $user_policy"
fi
