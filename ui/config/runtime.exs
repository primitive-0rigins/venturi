import Config

# config/runtime.exs is executed for all environments, including
# during releases. It is executed after compilation and before the
# system starts, so it is typically used to load production configuration
# and secrets from environment variables or elsewhere. Do not define
# any compile-time configuration in here, as it won't be applied.
# The block below contains prod specific runtime configuration.

# ## Using releases
#
# If you use `mix release`, you need to explicitly enable the server
# by passing the PHX_SERVER=true when you start it:
#
#     PHX_SERVER=true bin/venturi_ui start
#
# Alternatively, you can use `mix phx.gen.release` to generate a `bin/server`
# script that automatically sets the env var above.
if System.get_env("PHX_SERVER") do
  config :venturi_ui, VenturiUiWeb.Endpoint, server: true
end

if base_url = System.get_env("VENTURI_API_URL") do
  config :venturi_ui, :venturi_api, base_url: base_url
end

if api_key = System.get_env("VENTURI_API_KEY") do
  config :venturi_ui, :venturi_api, api_key: api_key
end

if namespace = System.get_env("VENTURI_NAMESPACE") do
  config :venturi_ui, :venturi_api, namespace: namespace
end

if config_env() == :prod do
  # The secret key base is used to sign/encrypt cookies and other secrets.
  # A default value is used in config/dev.exs and config/test.exs but you
  # want to use a different value for prod and you most likely don't want
  # to check this value into version control, so we use an environment
  # variable instead.
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  host = System.get_env("PHX_HOST") || "example.com"
  port = String.to_integer(System.get_env("PORT") || "4000")

  issuer = System.fetch_env!("VENTURI_UI_OIDC_ISSUER")
  client_id = System.fetch_env!("VENTURI_UI_OIDC_CLIENT_ID")

  redirect_uri =
    System.get_env("VENTURI_UI_OIDC_REDIRECT_URI") || "https://#{host}/auth/oidc/callback"

  config :venturi_ui, :dns_cluster_query, System.get_env("DNS_CLUSTER_QUERY")

  config :venturi_ui, :oidc,
    issuer: issuer,
    client_id: client_id,
    client_secret: System.get_env("VENTURI_UI_OIDC_CLIENT_SECRET"),
    client_authentication_method:
      if(System.get_env("VENTURI_UI_OIDC_CLIENT_SECRET"), do: :client_secret_basic, else: :none),
    redirect_uri: redirect_uri,
    group_claim: System.get_env("VENTURI_UI_OIDC_GROUP_CLAIM") || "groups",
    operator_groups:
      String.split(System.get_env("VENTURI_UI_OIDC_OPERATOR_GROUPS") || "venturi-operator", ",",
        trim: true
      ),
    auditor_groups:
      String.split(System.get_env("VENTURI_UI_OIDC_AUDITOR_GROUPS") || "venturi-auditor", ",",
        trim: true
      ),
    session_ttl_seconds:
      String.to_integer(System.get_env("VENTURI_UI_SESSION_TTL_SECONDS") || "900")

  config :venturi_ui, VenturiUiWeb.Endpoint,
    url: [host: host, port: 443, scheme: "https"],
    # Keep the dashboard private; terminate TLS and proxy from a local reverse proxy.
    http: [ip: {127, 0, 0, 1}, port: port],
    force_ssl: [rewrite_on: [:x_forwarded_proto], hsts: true],
    secret_key_base: secret_key_base

  # ## SSL Support
  #
  # To get SSL working, you will need to add the `https` key
  # to your endpoint configuration:
  #
  #     config :venturi_ui, VenturiUiWeb.Endpoint,
  #       https: [
  #         ...,
  #         port: 443,
  #         cipher_suite: :strong,
  #         keyfile: System.get_env("SOME_APP_SSL_KEY_PATH"),
  #         certfile: System.get_env("SOME_APP_SSL_CERT_PATH")
  #       ]
  #
  # The `cipher_suite` is set to `:strong` to support only the
  # latest and more secure SSL ciphers. This means old browsers
  # and clients may not be supported. You can set it to
  # `:compatible` for wider support.
  #
  # `:keyfile` and `:certfile` expect an absolute path to the key
  # and cert in disk or a relative path inside priv, for example
  # "priv/ssl/server.key". For all supported SSL configuration
  # options, see https://hexdocs.pm/plug/Plug.SSL.html#configure/1
  #
  # We also recommend setting `force_ssl` in your config/prod.exs,
  # ensuring no data is ever sent via http, always redirecting to https:
  #
  #     config :venturi_ui, VenturiUiWeb.Endpoint,
  #       force_ssl: [hsts: true]
  #
  # Check `Plug.SSL` for all available options in `force_ssl`.
end
