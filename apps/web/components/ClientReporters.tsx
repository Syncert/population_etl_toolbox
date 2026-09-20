"use client";

// The three voices, wired once.
//
// Everything this file decides is in `lib/clientReport`: what a report may
// say, how it is built and how it is sent. What is here is the wiring --
// three listeners and a hook -- registered in the root layout so every route
// has them and no route has two.
//
// The route comes from `usePathname` rather than from `location.href`: the
// pathname is what a report may carry, so reading it from the router means
// the query string is never in hand to leak in the first place.

import { useEffect } from "react";
import { usePathname } from "next/navigation";
import { useReportWebVitals } from "next/web-vitals";

import {
  errorReport,
  rejectionReport,
  sendReport,
  violationReport,
  vitalReport,
} from "../lib/clientReport";

export default function ClientReporters() {
  const pathname = usePathname() || "/";

  useReportWebVitals((metric) => {
    sendReport(vitalReport(pathname, metric));
  });

  useEffect(() => {
    const onError = (event: ErrorEvent) => {
      sendReport(errorReport(pathname, event));
    };
    const onRejection = (event: PromiseRejectionEvent) => {
      sendReport(rejectionReport(pathname, event.reason));
    };
    // The browser's own violation event, beside the `report-to` directive in
    // `middleware.ts`. They are not redundant: the directive reaches an
    // operator from a reader whose browser implements the Reporting API, and
    // this event fires in every browser that implements CSP at all.
    const onViolation = (event: SecurityPolicyViolationEvent) => {
      sendReport(violationReport(pathname, event));
    };

    window.addEventListener("error", onError);
    window.addEventListener("unhandledrejection", onRejection);
    document.addEventListener("securitypolicyviolation", onViolation);
    return () => {
      window.removeEventListener("error", onError);
      window.removeEventListener("unhandledrejection", onRejection);
      document.removeEventListener("securitypolicyviolation", onViolation);
    };
  }, [pathname]);

  return null;
}
