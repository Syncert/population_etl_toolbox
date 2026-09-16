"use client";

// The boundary for a failure in the root layout itself.
//
// `app/error.js` renders *inside* the layout, so it cannot catch a throw from
// the layout that would contain it. This one replaces the document, which is
// why it carries its own `html` and `body`: at this point there is no shell
// left to render into, and referencing the site's stylesheet classes would be
// styling elements the failed layout never mounted. So it is deliberately
// plain, and deliberately still has `main` and one `h1`.

export default function GlobalError({ error, reset }) {
  return (
    <html lang="en">
      <body>
        <main data-testid="global-error">
          <h1>Economic Data Studio could not start</h1>
          <p>
            The application failed before any page could be drawn.
            {error?.digest ? ` Quote ${error.digest} if you report it.` : ""}
          </p>
          <button type="button" onClick={() => reset()}>
            Reload the application
          </button>
        </main>
      </body>
    </html>
  );
}
