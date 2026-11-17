"use client";

// The place page's folds (website UX review C1; Nick 2026-11-16: "Fold
// detail, headlines stay open"). Each chapter shows its headline cards and
// keeps its charts, small figures and special sections in one
// `<details class="place-chapter-more">`. This is everything about the folds
// that is not the markup itself, in its own chunk:
//
// - one switch that opens or folds every chapter, remembered in this browser;
// - a link to something inside a fold opens that fold first, on arrival and
//   on every in-page link, so no address on the page leads to a closed box;
// - printing opens every fold.

import { useEffect } from "react";

const KEY = "place-detail";
const FOLD = "details.place-chapter-more";

/**
 * Open the fold around the element a fragment names, then bring it into
 * view. False while nothing on the page has that id yet.
 */
function reveal(hash: string): boolean {
  let target: Element | null = null;
  try {
    target = hash.length > 1 ? document.getElementById(decodeURIComponent(hash.slice(1))) : null;
  } catch {
    return true;
  }
  if (!target) return hash.length <= 1;
  const fold = target.closest<HTMLDetailsElement>(FOLD);
  if (fold && !fold.open) {
    fold.open = true;
    target.scrollIntoView();
  }
  return true;
}

export default function PlaceFolds({ open, onChange }: { open: boolean; onChange: (open: boolean) => void }) {
  useEffect(() => {
    // The address may name a chart that is drawn only once its figures have
    // been read: keep looking until it exists, for at most half a minute.
    let waiting: MutationObserver | null = null;
    if (!reveal(window.location.hash)) {
      waiting = new MutationObserver(() => {
        if (reveal(window.location.hash)) waiting?.disconnect();
      });
      waiting.observe(document.body, { childList: true, subtree: true });
      window.setTimeout(() => waiting?.disconnect(), 30000);
    }
    const onHash = () => reveal(window.location.hash);
    // A link written with the router changes the address without a
    // `hashchange`, so the click itself is read too.
    const onClick = (event: MouseEvent) => {
      const link = (event.target as Element | null)?.closest?.("a[href^='#']");
      if (link) reveal(link.getAttribute("href") || "");
    };
    const onPrint = () => document.querySelectorAll<HTMLDetailsElement>(FOLD).forEach((fold) => { fold.open = true; });
    window.addEventListener("hashchange", onHash);
    document.addEventListener("click", onClick);
    window.addEventListener("beforeprint", onPrint);
    return () => {
      waiting?.disconnect();
      window.removeEventListener("hashchange", onHash);
      document.removeEventListener("click", onClick);
      window.removeEventListener("beforeprint", onPrint);
    };
  }, []);

  const toggle = () => {
    const next = !open;
    try {
      window.localStorage.setItem(KEY, next ? "open" : "folded");
    } catch {
      // The choice still holds for this page.
    }
    // The switch speaks for every fold, including one the reader moved by hand.
    document.querySelectorAll<HTMLDetailsElement>(FOLD).forEach((fold) => { fold.open = next; });
    onChange(next);
  };

  return (
    <p className="place-folds" data-testid="place-folds">
      Each chapter shows its headline figures. Its charts and further figures are under &ldquo;More about&rdquo;.{" "}
      <button type="button" className="text-button" aria-pressed={open} onClick={toggle} data-testid="place-folds-toggle">
        {open ? "Fold every chapter's detail" : "Open every chapter's detail"}
      </button>
    </p>
  );
}
