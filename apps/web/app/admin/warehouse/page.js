import AdminWarehousePage from "../../../components/AdminWarehousePage";
import { STATIC_ROUTE_TITLES } from "../../../lib/routeTitles";
import "./admin-warehouse.css";

// A server wrapper, so this route can name itself and stay out of search
// engines. Admin only: the API refuses every read for an account not on the
// deployment's admin list, and this page only explains that answer. Its CSS
// and component load with this route alone, never with a public page.
export const metadata = {
  title: STATIC_ROUTE_TITLES["/admin/warehouse"],
  robots: { index: false, follow: false },
};

export default function AdminWarehouse() {
  return <AdminWarehousePage />;
}
