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

  test "GET / requires operator credentials when configured" do
    previous = Application.get_env(:venturi_ui, :operator_auth)

    Application.put_env(:venturi_ui, :operator_auth,
      username: "operator",
      password: "test-password"
    )

    on_exit(fn -> Application.put_env(:venturi_ui, :operator_auth, previous) end)

    assert get(build_conn(), ~p"/").status == 401

    Req.Test.stub(VenturiUi.VenturiClient, fn conn ->
      Req.Test.json(conn, %{"ok" => true, "capabilities" => %{}})
    end)

    conn =
      build_conn()
      |> put_req_header(
        "authorization",
        Plug.BasicAuth.encode_basic_auth("operator", "test-password")
      )
      |> get(~p"/")

    assert html_response(conn, 200) =~ "Online"
  end
end
