defmodule VenturiUiWeb.DashboardControllerTest do
  use VenturiUiWeb.ConnCase, async: false

  test "GET / renders capability status", %{conn: conn} do
    Req.Test.stub(VenturiUi.VenturiClient, fn conn ->
      Req.Test.json(conn, %{
        "ok" => true,
        "capabilities" => %{"embedding" => "ready", "graph" => "degraded"}
      })
    end)

    conn = get(conn, ~p"/")

    assert html_response(conn, 200) =~ "Online"
    assert html_response(conn, 200) =~ "embedding"
    assert html_response(conn, 200) =~ "degraded"
  end

  test "GET / surfaces a transport error", %{conn: conn} do
    Req.Test.stub(VenturiUi.VenturiClient, fn conn ->
      Req.Test.transport_error(conn, :econnrefused)
    end)

    conn = get(conn, ~p"/")

    assert html_response(conn, 200) =~ "Could not reach Venturi"
  end

  test "expired OIDC session is redirected to sign-in", %{conn: conn} do
    Application.put_env(:venturi_ui, :oidc_test_bypass, false)
    on_exit(fn -> Application.put_env(:venturi_ui, :oidc_test_bypass, true) end)

    conn =
      conn
      |> init_test_session(%{role: "auditor", authenticated_at: 0})
      |> get(~p"/")

    assert redirected_to(conn) == "/auth/oidc"
  end

  test "auditor cannot perform operator hold actions", %{conn: conn} do
    Application.put_env(:venturi_ui, :oidc_test_bypass, false)
    on_exit(fn -> Application.put_env(:venturi_ui, :oidc_test_bypass, true) end)

    conn =
      conn
      |> init_test_session(%{role: "auditor", authenticated_at: System.system_time(:second)})
      |> post(~p"/holds", %{"parent_id" => "chain", "namespace" => "clinical", "reason" => "test"})

    assert response(conn, 403) == "operator role required"
  end

  test "OIDC callback without a valid authorization response is denied", %{conn: conn} do
    conn = get(conn, ~p"/auth/oidc/callback")

    assert redirected_to(conn) == "/"
    assert Phoenix.Flash.get(conn.assigns.flash, :error) == "Sign-in was denied"
  end
end
