import { STATIC_ROUTE_TITLES } from "../../lib/routeTitles";

// `/catalog` is not a thin wrapper -- it is the screen itself, and a client
// component cannot export `metadata`. A segment layout can, and a layout that
// renders only its children changes nothing about how the page renders. The
// alternative was splitting a working client page in two to give it a title.
export const metadata = { title: STATIC_ROUTE_TITLES["/catalog"] };

export default function CatalogLayout({ children }) {
  return children;
}
