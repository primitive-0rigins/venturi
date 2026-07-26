defmodule VenturiUiWeb.Router do
  use VenturiUiWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {VenturiUiWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :authenticated do
    plug :require_authenticated
  end

  pipeline :operator do
    plug :require_operator
  end

  defp require_authenticated(conn, _opts) do
    if Application.get_env(:venturi_ui, :oidc_test_bypass, false),
      do: conn,
      else: require_oidc_session(conn)
  end

  defp require_oidc_session(conn) do
    ttl = Application.fetch_env!(:venturi_ui, :oidc) |> Keyword.fetch!(:session_ttl_seconds)

    if is_binary(get_session(conn, :role)) and
         System.system_time(:second) - (get_session(conn, :authenticated_at) || 0) < ttl,
       do: conn,
       else: conn |> configure_session(drop: true) |> redirect(to: "/auth/oidc") |> halt()
  end

  defp require_operator(conn, _opts) do
    if Application.get_env(:venturi_ui, :oidc_test_bypass, false) or
         get_session(conn, :role) == "operator",
       do: conn,
       else: conn |> send_resp(403, "operator role required") |> halt()
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", VenturiUiWeb do
    pipe_through [:browser]
    get "/auth/oidc", OidcController, :new
    get "/auth/oidc/callback", OidcController, :callback
    delete "/auth/logout", OidcController, :delete
  end

  scope "/", VenturiUiWeb do
    pipe_through [:browser, :authenticated]

    get "/", DashboardController, :home

    get "/audit", AuditController, :new

    get "/chains", ChainController, :index
    get "/holds", HoldController, :index
  end

  scope "/", VenturiUiWeb do
    pipe_through [:browser, :authenticated, :operator]
    post "/chains/link", ChainController, :create_link
    post "/holds", HoldController, :create
    delete "/holds", HoldController, :delete
  end

  # Other scopes may use custom stacks.
  # scope "/api", VenturiUiWeb do
  #   pipe_through :api
  # end
end
