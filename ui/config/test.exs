import Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :venturi_ui, VenturiUiWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "hmlNsgzxnDWLE9XrHyaCqoTNAJSKi7q6DhXamquBkTLDbOcuBLy+qg9RClnJ2tPX",
  server: false

# Route the Venturi API client through Req.Test instead of a real HTTP call.
config :venturi_ui, :venturi_api,
  base_url: "http://venturi.test",
  plug: {Req.Test, VenturiUi.VenturiClient}

# Print only warnings and errors during test
config :logger, level: :warning

# Initialize plugs at runtime for faster test compilation
config :phoenix, :plug_init_mode, :runtime

# Enable helpful, but potentially expensive runtime checks
config :phoenix_live_view,
  enable_expensive_runtime_checks: true
