import { chromium, expect, test } from "@playwright/test";
import path from "node:path";

test("explicit fixture action opens the canonical PWA verdict", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Start fixture demo" }).click();

  await expect(page).toHaveURL(/\/claims\/hero-ev-lifecycle-2026$/);
  await expect(page.getByRole("heading", { name: "Misleading" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Sources" })).toBeVisible();
  await expect(page.locator("article li")).toHaveCount(3);
});

test("iPhone PWA metadata and one-time pairing work against the API", async ({ page, request }) => {
  const manifest = await request.get("http://127.0.0.1:5173/manifest.webmanifest");
  expect(manifest.ok()).toBeTruthy();
  const metadata = await manifest.json();
  expect(metadata.display).toBe("standalone");
  expect(metadata.icons).toHaveLength(1);

  const sessionResponse = await request.post("http://127.0.0.1:8000/v1/sessions", {
    data: { idempotency_key: `browser-pair-${Date.now()}`, fixture_mode: true },
  });
  const session = await sessionResponse.json();
  const pairingResponse = await request.post("http://127.0.0.1:8000/v1/pairings", {
    data: { session_id: session.id },
  });
  const pairing = await pairingResponse.json();

  await page.goto("/");
  await page.getByLabel("Pairing code").fill(pairing.code);
  await page.getByRole("button", { name: "Pair device" }).click();
  await expect(page.getByText("Paired — enable notifications")).toBeVisible();
  await expect(page.getByRole("button", { name: "Enable notifications" })).toBeVisible();

  await page.evaluate(() => localStorage.clear());
  await page.reload();
  await page.getByLabel("Pairing code").fill(pairing.code);
  await page.getByRole("button", { name: "Pair device" }).click();
  await expect(page.getByText("That pairing code is invalid, expired, or already used")).toBeVisible();
});

test("the production MV3 bundle loads as an unpacked extension", async ({}, testInfo) => {
  const extension = path.resolve("apps/extension/dist");
  const context = await chromium.launchPersistentContext(testInfo.outputPath("profile"), {
    channel: "chromium",
    headless: true,
    args: [
      `--disable-extensions-except=${extension}`,
      `--load-extension=${extension}`,
    ],
  });

  try {
    const worker = context.serviceWorkers()[0] ?? await context.waitForEvent("serviceworker");
    expect(worker.url()).toMatch(/^chrome-extension:\/\/.+\/worker\.js$/);
    const popup = await context.newPage();
    await popup.goto(new URL("popup.html", worker.url()).href);
    await expect(popup.getByRole("button", { name: "Start live listening" })).toBeVisible();
    await expect(popup.getByRole("button", { name: "Demo fallback" })).toBeVisible();
  } finally {
    await context.close();
  }
});
