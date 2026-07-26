defmodule VenturiUiWeb.ChainControllerTest do
  use VenturiUiWeb.ConnCase, async: false

  test "GET /chains renders the lookup and link forms", %{conn: conn} do
    conn = get(conn, ~p"/chains")
    body = html_response(conn, 200)
    assert body =~ "Chain References"
    assert body =~ "Declare a Reference"
  end

  test "GET /chains?parent_id=... lists references", %{conn: conn} do
    Req.Test.stub(VenturiUi.VenturiClient, fn conn ->
      Req.Test.json(conn, %{
        "references" => [
          %{
            "from_parent_id" => "chain-a",
            "to_parent_id" => "chain-b",
            "reference_type" => "supersedes",
            "created_at" => "1753142400Z"
          }
        ],
        "count" => 1
      })
    end)

    conn = get(conn, ~p"/chains?parent_id=chain-a")

    body = html_response(conn, 200)
    assert body =~ "chain-a"
    assert body =~ "chain-b"
    assert body =~ "supersedes"
  end

  test "POST /chains/link creates a link and redirects back with the from parent", %{conn: conn} do
    Req.Test.stub(VenturiUi.VenturiClient, fn conn ->
      Req.Test.json(conn, %{"ok" => true})
    end)

    conn =
      post(conn, ~p"/chains/link", %{
        "link" => %{
          "from_parent_id" => "chain-a",
          "to_parent_id" => "chain-b",
          "reference_type" => "cites"
        }
      })

    assert redirected_to(conn) == ~p"/chains?#{[parent_id: "chain-a"]}"
    assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "chain-a -> chain-b"
  end

  test "POST /chains/link rejects malformed form data", %{conn: conn} do
    conn = post(conn, ~p"/chains/link", %{"link" => %{}})

    assert redirected_to(conn) == ~p"/chains"
    assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "required"
  end
end
