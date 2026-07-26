defmodule VenturiUiWeb.OidcController do
  use VenturiUiWeb, :controller

  alias Assent.Strategy.OIDC

  def new(conn, _params) do
    case OIDC.authorize_url(config()) do
      {:ok, %{url: url, session_params: params}} ->
        conn |> put_session(:oidc_params, params) |> redirect(external: url)

      {:error, _} ->
        conn |> put_flash(:error, "Unable to start sign-in") |> redirect(to: ~p"/")
    end
  end

  def callback(conn, params) do
    oidc = config() |> Keyword.put(:session_params, get_session(conn, :oidc_params))
    conn = delete_session(conn, :oidc_params)

    with {:ok, %{user: user}} <- OIDC.callback(oidc, params),
         {:ok, role} <- role(user) do
      conn
      |> configure_session(renew: true)
      |> put_session(:oidc_user, Map.get(user, "sub"))
      |> put_session(:role, role)
      |> put_session(:authenticated_at, System.system_time(:second))
      |> redirect(to: ~p"/")
    else
      _ -> conn |> put_flash(:error, "Sign-in was denied") |> redirect(to: ~p"/")
    end
  end

  def delete(conn, _params) do
    conn |> configure_session(drop: true) |> redirect(to: ~p"/")
  end

  defp role(user) do
    opts = Application.fetch_env!(:venturi_ui, :oidc)
    groups = List.wrap(Map.get(user, Keyword.fetch!(opts, :group_claim), []))

    cond do
      Enum.any?(groups, &(&1 in Keyword.fetch!(opts, :operator_groups))) -> {:ok, "operator"}
      Enum.any?(groups, &(&1 in Keyword.fetch!(opts, :auditor_groups))) -> {:ok, "auditor"}
      true -> :error
    end
  end

  defp config do
    opts = Application.fetch_env!(:venturi_ui, :oidc)

    [
      client_id: Keyword.fetch!(opts, :client_id),
      base_url: Keyword.fetch!(opts, :issuer),
      redirect_uri: Keyword.fetch!(opts, :redirect_uri),
      client_authentication_method: Keyword.fetch!(opts, :client_authentication_method),
      client_secret: Keyword.get(opts, :client_secret),
      authorization_params: [scope: "openid profile email"],
      code_verifier: true,
      nonce: true,
      id_token_ttl_seconds: Keyword.fetch!(opts, :session_ttl_seconds)
    ]
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end
end
