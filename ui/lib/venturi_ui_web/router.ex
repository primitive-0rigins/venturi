defmodule VenturiUiWeb.Router do
  use VenturiUiWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {VenturiUiWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug :require_operator
  end

  defp require_operator(conn, _opts) do
    case Application.fetch_env!(:venturi_ui, :operator_auth) do
      [username: username, password: password] ->
        Plug.BasicAuth.basic_auth(conn, username: username, password: password, realm: "Venturi")

      _ ->
        conn
    end
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", VenturiUiWeb do
    pipe_through :browser

    get "/", DashboardController, :home

    get "/audit", AuditController, :new

    get "/chains", ChainController, :index
    post "/chains/link", ChainController, :create_link

    get "/holds", HoldController, :index
    post "/holds", HoldController, :create
    delete "/holds", HoldController, :delete
  end

  # Other scopes may use custom stacks.
  # scope "/api", VenturiUiWeb do
  #   pipe_through :api
  # end
end
