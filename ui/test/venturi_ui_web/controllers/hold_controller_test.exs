defmodule VenturiUiWeb.HoldControllerTest do
  use VenturiUiWeb.ConnCase, async: false

  test "GET /holds renders the place and release forms", %{conn: conn} do
    conn = get(conn, ~p"/holds")
    body = html_response(conn, 200)
    assert body =~ "Legal Hold"
    assert body =~ "Release a Hold"
  end

  test "POST /holds places a hold and redirects with a flash", %{conn: conn} do
    Req.Test.stub(VenturiUi.VenturiClient, fn conn ->
      Req.Test.json(conn, %{"ok" => true})
    end)

    conn =
      post(conn, ~p"/holds", %{"hold" => %{"parent_id" => "chain-a", "reason" => "litigation"}})

    assert redirected_to(conn) == ~p"/holds"
    assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "chain-a"
  end

  test "DELETE /holds releases a hold and redirects with a flash", %{conn: conn} do
    Req.Test.stub(VenturiUi.VenturiClient, fn conn ->
      Req.Test.json(conn, %{"ok" => true})
    end)

    conn = delete(conn, ~p"/holds", %{"release" => %{"parent_id" => "chain-a"}})

    assert redirected_to(conn) == ~p"/holds"
    assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "released"
  end

  test "POST /holds surfaces an API error", %{conn: conn} do
    Req.Test.stub(VenturiUi.VenturiClient, fn conn ->
      conn |> Plug.Conn.put_status(403) |> Req.Test.json(%{"error" => "forbidden"})
    end)

    conn =
      post(conn, ~p"/holds", %{"hold" => %{"parent_id" => "chain-a", "reason" => "litigation"}})

    assert redirected_to(conn) == ~p"/holds"
    assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "API returned 403"
  end

  test "hold actions reject malformed form data", %{conn: conn} do
    create = post(conn, ~p"/holds", %{})
    assert redirected_to(create) == ~p"/holds"
    assert Phoenix.Flash.get(create.assigns.flash, :error) =~ "required"

    release = delete(conn, ~p"/holds", %{})
    assert redirected_to(release) == ~p"/holds"
    assert Phoenix.Flash.get(release.assigns.flash, :error) =~ "required"
  end
end
