import ProfileProduct from "../../components/ProfileProduct";
import { profileTitle } from "../../lib/routeTitles";

export async function generateMetadata({ searchParams }) {
  return { title: profileTitle(await searchParams) };
}

export default function ProfilesPage() {
  return <ProfileProduct />;
}
