import SignInCallback from "../../../components/SignInCallback";

// A server wrapper, so this route can name itself. Its address carries an
// authorization code for the first instant of its life and nothing after
// that, so the title is fixed and says nothing about it.
export const metadata = {
  title: "Signing in",
  // Never indexed. The address exists only as a provider's redirect target;
  // a crawler that found it would fetch it without a code and be told the
  // link was incomplete, which is a page nobody should be shown from search.
  robots: { index: false, follow: false },
};

export default function SignInCallbackPage() {
  return <SignInCallback />;
}
