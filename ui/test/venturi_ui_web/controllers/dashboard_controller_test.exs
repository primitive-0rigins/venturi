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
end
