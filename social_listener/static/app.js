const form = document.getElementById("search-form");
const searchCacheButton = document.getElementById("search-cache");
const refreshBrandButton = document.getElementById("refresh-brand");
const createBrandButton = document.getElementById("create-brand");
const brandCreateForm = document.getElementById("brand-create-form");
const brandFilterInput = document.getElementById("brand-filter-input");
const brandList = document.getElementById("brand-list");
const brandCount = document.getElementById("brand-count");
const newBrandNameInput = document.getElementById("new-brand-name");
const newBrandQueryInput = document.getElementById("new-brand-query");
const activeBrandName = document.getElementById("active-brand-name");
const activeBrandSubtitle = document.getElementById("active-brand-subtitle");
const filterPanel = document.getElementById("filter-panel");
const resultsSection = document.getElementById("results-section");
const resultsList = document.getElementById("results-list");
const warningsBox = document.getElementById("warnings");
const resultCount = document.getElementById("result-count");
const platformCount = document.getElementById("platform-count");
const statusText = document.getElementById("status-text");
const termPill = document.getElementById("term-pill");

const state = {
  brands: [],
  activeBrandId: null,
  requestToken: 0,
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function sortBrands(items) {
  return [...items].sort((left, right) => left.name.localeCompare(right.name, "tr"));
}

function getActiveBrand() {
  return state.brands.find((brand) => brand.id === state.activeBrandId) || null;
}

function setBrandWorkspaceEnabled(isEnabled) {
  form.querySelectorAll("input, textarea, button").forEach((element) => {
    element.disabled = !isEnabled;
  });
  refreshBrandButton.disabled = !isEnabled;
  filterPanel.classList.toggle("workspace-disabled", !isEnabled);
  resultsSection.classList.toggle("workspace-disabled", !isEnabled);
}

function setBusy(isBusy, message) {
  document.querySelectorAll("button").forEach((button) => {
    button.disabled = isBusy;
  });
  statusText.textContent = message;
}

function renderWarnings(warnings) {
  if (!warnings || warnings.length === 0) {
    warningsBox.innerHTML = "";
    warningsBox.style.display = "none";
    return;
  }

  warningsBox.style.display = "block";
  warningsBox.innerHTML = warnings
    .map((warning) => `<div class="warning-item">${escapeHtml(warning)}</div>`)
    .join("");
}

function toDateTimeLocalValue(rawValue) {
  if (!rawValue) {
    return "";
  }

  const date = new Date(rawValue);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  const pad = (value) => String(value).padStart(2, "0");
  return [
    date.getFullYear(),
    pad(date.getMonth() + 1),
    pad(date.getDate()),
  ].join("-") + `T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function selectedPlatforms() {
  return Array.from(document.querySelectorAll('input[name="platform"]:checked')).map((input) => input.value);
}

function setSelectedPlatforms(platforms) {
  const activePlatforms = Array.isArray(platforms) && platforms.length ? platforms : ["youtube"];
  document.querySelectorAll('input[name="platform"]').forEach((input) => {
    input.checked = activePlatforms.includes(input.value);
  });
}

function currentFormProfile() {
  return {
    name: activeBrandName.textContent.trim(),
    query_text: document.getElementById("query").value.trim(),
    requested_from: document.getElementById("from").value || null,
    requested_to: document.getElementById("to").value || null,
    platforms: selectedPlatforms(),
  };
}

function formPayloadFromProfile(profile) {
  return {
    query: profile.query_text,
    from: profile.requested_from,
    to: profile.requested_to,
    platforms: profile.platforms,
  };
}

function formatNumber(value) {
  if (value === null || value === undefined || value === "") {
    return "Yok";
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return "Yok";
  }

  return new Intl.NumberFormat("tr-TR").format(parsed);
}

function formatRelativeTime(value) {
  if (!value) {
    return "tarih yok";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "tarih yok";
  }

  const diffMs = Math.max(0, Date.now() - date.getTime());
  const diffMinutes = Math.floor(diffMs / 60000);
  if (diffMinutes < 1) {
    return "az once";
  }
  if (diffMinutes < 60) {
    return `${diffMinutes} dk once`;
  }

  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) {
    return `${diffHours} saat once`;
  }

  const diffDays = Math.floor(diffHours / 24);
  if (diffDays < 30) {
    return `${diffDays} gun once`;
  }

  const diffMonths = Math.floor(diffDays / 30);
  if (diffMonths < 12) {
    return `${diffMonths} ay once`;
  }

  const diffYears = Math.floor(diffMonths / 12);
  return `${diffYears} yil once`;
}

function clipText(value, limit = 280) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  if (text.length <= limit) {
    return text;
  }
  return `${text.slice(0, limit - 1).trimEnd()}...`;
}

function platformLabel(platform) {
  const labels = {
    youtube: "YouTube",
    instagram: "Instagram",
    facebook: "Facebook",
    linkedin: "LinkedIn",
  };
  return labels[platform] || platform;
}

function activityLabel(item) {
  if (item.content_type === "video") {
    return "video paylasti";
  }
  if (item.content_type === "comment") {
    return "yorum yapti";
  }
  if (item.content_type === "comment-reply") {
    return "yanit verdi";
  }
  return "paylasim yapti";
}

function primaryActor(item) {
  if ((item.content_type === "comment" || item.content_type === "comment-reply") && item.author_name) {
    return item.author_name;
  }
  return item.source_name || item.author_name || "Bilinmeyen hesap";
}

function secondarySource(item) {
  return item.source_name || item.author_name || "Bilinmeyen kaynak";
}

function displayTitle(item) {
  if (item.content_type === "comment" || item.content_type === "comment-reply") {
    return clipText(item.body_text || item.title || "Yorum bulunamadi", 220);
  }
  return clipText(item.title || item.body_text || "Basliksiz icerik", 180);
}

function displayBody(item) {
  if (item.content_type === "comment" || item.content_type === "comment-reply") {
    return clipText(item.title || secondarySource(item), 180);
  }
  return clipText(item.body_text || "Icerik ozeti bulunmuyor.", 260);
}

function avatarText(item) {
  const source = primaryActor(item);
  const pieces = source
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase() || "");
  return pieces.join("") || platformLabel(item.platform).slice(0, 2).toUpperCase();
}

function metricDisplayValue(item, field) {
  const value = item[field];
  if (value !== null && value !== undefined && value !== "") {
    return formatNumber(value);
  }

  if (item.platform === "youtube" && field === "dislike_count") {
    return "Gizli";
  }

  if (item.platform === "youtube" && field === "channel_subscriber_count") {
    return "Gizli / yok";
  }

  return "Yok";
}

function itemTags(item) {
  const tags = ["checked"];
  tags.push(item.is_read ? "read" : "new");
  if (item.source_kind === "owned-channel") {
    tags.push("official");
  }
  return tags;
}

function matchSummary(item, terms) {
  if (!terms.length) {
    return "eslesme yok";
  }
  const suffix = item.source_kind === "owned-channel" ? ", resmi kanal" : "";
  return `${terms.join(", ")}${suffix}`;
}

function mergeBrand(updatedBrand) {
  const exists = state.brands.some((brand) => brand.id === updatedBrand.id);
  state.brands = sortBrands(
    exists
      ? state.brands.map((brand) => (brand.id === updatedBrand.id ? updatedBrand : brand))
      : [...state.brands, updatedBrand]
  );
}

function updateActiveBrandHeader(brand) {
  if (!brand) {
    activeBrandName.textContent = "Uygulamaya hos geldiniz";
    activeBrandSubtitle.textContent = "Sol taraftan ilgili markayi secin; eger listede yoksa Marka ekle alanindan yeni marka olusturun. Marka secilene kadar veri ekrani pasif bekler.";
    return;
  }

  activeBrandName.textContent = brand.name;
  const platformText = (brand.platforms || []).map((platform) => platformLabel(platform)).join(", ");
  activeBrandSubtitle.textContent = `Kayitli filtre: ${brand.query_text} | Platformlar: ${platformText || "Yok"} | Son bulunan kayit: ${brand.last_result_count || 0}`;
}

function populateBrandForm(brand) {
  document.getElementById("query").value = brand?.query_text || "";
  document.getElementById("from").value = toDateTimeLocalValue(brand?.requested_from);
  document.getElementById("to").value = toDateTimeLocalValue(brand?.requested_to);
  setSelectedPlatforms(brand?.platforms || ["youtube"]);
}

function renderWelcomeState() {
  resultCount.textContent = "0";
  platformCount.textContent = "0";
  termPill.textContent = "Marka secildiginde filtreler burada gorunecek.";
  resultsList.innerHTML = `
    <article class="empty-state">
      <p>Sol taraftan bir marka secin ya da yeni marka ekleyin. Marka secildiginde kayitli filtre paneli acilir ve sonuc akisi yuklenir.</p>
    </article>
  `;
}

function renderBrandList() {
  const filterText = brandFilterInput.value.trim().toLocaleLowerCase("tr");
  const filteredBrands = state.brands.filter((brand) => {
    if (!filterText) {
      return true;
    }
    return brand.name.toLocaleLowerCase("tr").includes(filterText);
  });

  brandCount.textContent = String(filteredBrands.length);

  if (filteredBrands.length === 0) {
    brandList.innerHTML = '<article class="brand-empty">Bu aramaya uyan marka yok.</article>';
    return;
  }

  brandList.innerHTML = filteredBrands
    .map((brand) => `
      <button
        type="button"
        class="brand-row ${brand.id === state.activeBrandId ? "brand-row-active" : ""}"
        data-brand-id="${brand.id}"
      >
        <span class="brand-row-title">${escapeHtml(brand.name)}</span>
        <span class="brand-row-query">${escapeHtml(clipText(brand.query_text, 54))}</span>
        <span class="brand-row-count">${escapeHtml(String(brand.last_result_count || 0))}</span>
      </button>
    `)
    .join("");
}

function renderResults(data) {
  const items = data.items || [];
  const terms = data.terms || [];
  resultCount.textContent = String(data.count ?? 0);
  platformCount.textContent = String(new Set(items.map((item) => item.platform)).size);
  termPill.textContent = terms.length ? terms.join(" | ") : "Terim bulunamadi";

  if (items.length === 0) {
    resultsList.innerHTML = `
      <article class="empty-state">
        <p>Bu filtre icin sonuc yok. Yeni veri denemek icin ustteki guncelle akisini kullanabilirsiniz.</p>
      </article>
    `;
    return;
  }

  resultsList.innerHTML = items
    .map((item) => {
      const title = displayTitle(item);
      const body = displayBody(item);
      const actor = primaryActor(item);
      const source = secondarySource(item);
      const url = item.permalink || item.content_url;
      const readClass = item.is_read ? "card-read-toggle-checked" : "";
      const readLabel = item.is_read ? "Okundu" : "Okunmadi";
      const shortId = item.external_id ? String(item.external_id).slice(-8) : "n/a";
      const tags = itemTags(item)
        .map((tag) => `<span class="side-tag">${escapeHtml(tag)}</span>`)
        .join("");
      const preview = item.thumbnail_url
        ? `<div class="result-preview"><img class="result-thumb" src="${escapeHtml(item.thumbnail_url)}" alt="${escapeHtml(title)} thumbnail" loading="lazy"></div>`
        : "";
      const storyBodyClass = preview ? "result-story-body" : "result-story-body result-story-body-no-media";

      return `
        <article class="result-card ${item.is_read ? "result-card-read" : ""}" data-item-id="${item.id}">
          <div class="result-main">
            <div class="result-avatar-shell">
              <div class="result-avatar">${escapeHtml(avatarText(item))}</div>
              <span class="result-platform-chip">${escapeHtml(platformLabel(item.platform))}</span>
            </div>

            <div class="result-story">
              <div class="result-heading">
                <span class="result-actor">${escapeHtml(actor)}</span>
                <span class="result-verb">${escapeHtml(activityLabel(item))}</span>
              </div>

              <div class="${storyBodyClass}">
                ${preview}
                <div class="result-copy">
                  <h3>${escapeHtml(title)}</h3>
                  <p>${escapeHtml(body)}</p>
                </div>
              </div>

              <div class="result-meta">
                <span>yayinlandi ${escapeHtml(formatRelativeTime(item.published_at))}</span>
                <span>${escapeHtml(source)}</span>
                <span>${escapeHtml(platformLabel(item.platform))}</span>
                <span>yakalandi ${escapeHtml(formatRelativeTime(item.last_seen_at))}</span>
                <span>ID ${escapeHtml(shortId)}</span>
              </div>
            </div>
          </div>

          <aside class="result-sidebox">
            <div class="sidebox-top">
              <span class="side-status-dot ${item.is_read ? "side-status-read" : "side-status-new"}"></span>
              <button
                type="button"
                class="card-read-toggle ${readClass}"
                data-read-toggle
                data-item-id="${item.id}"
                aria-pressed="${item.is_read ? "true" : "false"}"
                aria-label="${readLabel}"
                title="${readLabel}"
              >
                <span class="card-read-box">${item.is_read ? "✓" : ""}</span>
                <span class="card-read-text">Okundu</span>
              </button>
            </div>

            <div class="side-section">
              <span class="side-label">Tags</span>
              <div class="side-tags">${tags}</div>
            </div>

            <div class="side-section">
              <span class="side-label">Matches</span>
              <p class="side-copy">${escapeHtml(matchSummary(item, terms))}</p>
            </div>

            <div class="side-section">
              <span class="side-label">Metrics</span>
              <div class="side-metrics">
                <div class="side-metric">
                  <span>Yorum</span>
                  <strong>${escapeHtml(metricDisplayValue(item, "comment_count"))}</strong>
                </div>
                <div class="side-metric">
                  <span>Izlenme</span>
                  <strong>${escapeHtml(metricDisplayValue(item, "view_count"))}</strong>
                </div>
                <div class="side-metric">
                  <span>Begeni</span>
                  <strong>${escapeHtml(metricDisplayValue(item, "like_count"))}</strong>
                </div>
                <div class="side-metric">
                  <span>Dislike</span>
                  <strong>${escapeHtml(metricDisplayValue(item, "dislike_count"))}</strong>
                </div>
                <div class="side-metric side-metric-wide">
                  <span>Kanal abone sayisi</span>
                  <strong>${escapeHtml(metricDisplayValue(item, "channel_subscriber_count"))}</strong>
                </div>
              </div>
            </div>

            <a class="result-link" href="${escapeHtml(url)}" target="_blank" rel="noreferrer">Kaynagi ac</a>
          </aside>
        </article>
      `;
    })
    .join("");
}

function applyRangeDays(days) {
  if (days === null) {
    document.getElementById("from").value = "";
    document.getElementById("to").value = "";
    return;
  }

  const now = new Date();
  const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
  document.getElementById("from").value = toDateTimeLocalValue(start.toISOString());
  document.getElementById("to").value = toDateTimeLocalValue(now.toISOString());
}

async function fetchBrands(preferredBrandId = null) {
  const response = await fetch("/api/brands");
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "Markalar yuklenemedi");
  }

  state.brands = sortBrands(data.items || []);
  if (!state.brands.length) {
    state.activeBrandId = null;
    renderBrandList();
    updateActiveBrandHeader(null);
    populateBrandForm(null);
    setBrandWorkspaceEnabled(false);
    renderWelcomeState();
    return;
  }

  const nextBrandId =
    preferredBrandId ||
    state.activeBrandId ||
    null;
  state.activeBrandId = nextBrandId && state.brands.some((brand) => brand.id === nextBrandId) ? nextBrandId : null;
  renderBrandList();
}

async function searchBrandProfile(profile, options = {}) {
  const {
    brandId = null,
    token,
    busyMessage = "Kayitli veriler yukleniyor",
  } = options;
  if (token !== state.requestToken) {
    return null;
  }

  setBusy(true, busyMessage);
  const params = new URLSearchParams();
  params.set("query", profile.query_text);
  if (profile.requested_from) {
    params.set("from", profile.requested_from);
  }
  if (profile.requested_to) {
    params.set("to", profile.requested_to);
  }
  (profile.platforms || []).forEach((platform) => params.append("platform", platform));
  if (brandId) {
    params.set("brand_id", String(brandId));
  }

  const response = await fetch(`/api/search?${params.toString()}`);
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "Arama basarisiz");
  }
  if (token !== state.requestToken) {
    return null;
  }

  renderWarnings([]);
  renderResults(data);
  if (brandId) {
    const activeBrand = getActiveBrand();
    if (activeBrand) {
      mergeBrand({ ...activeBrand, last_result_count: data.count });
      renderBrandList();
      updateActiveBrandHeader(getActiveBrand());
    }
  }
  statusText.textContent = "Kayitli veriler gosterildi";
  setBusy(false, statusText.textContent);
  return data;
}

async function collectBrandProfile(profile, options = {}) {
  const {
    brandId = null,
    token,
  } = options;
  if (token !== state.requestToken) {
    return;
  }

  setBusy(true, "Kaynaklar taraniyor");
  const response = await fetch("/api/collect", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: profile.query_text,
      from: profile.requested_from,
      to: profile.requested_to,
      platforms: profile.platforms,
      brand_id: brandId,
    }),
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "Toplama basarisiz");
  }
  if (token !== state.requestToken) {
    return;
  }

  renderWarnings(data.warnings || []);
  statusText.textContent = "Yeni veri bulundu, ekran guncelleniyor";
  await searchBrandProfile(profile, { brandId, token, busyMessage: statusText.textContent });
}

async function queueBrandRefresh(profile, brandId, token) {
  try {
    await collectBrandProfile(profile, { brandId, token });
  } catch (error) {
    if (token === state.requestToken) {
      renderWarnings([error.message]);
      statusText.textContent = "Arka plan taramasi basarisiz";
      setBusy(false, statusText.textContent);
    }
  }
}

async function activateBrand(brandId, { refresh = true } = {}) {
  state.activeBrandId = brandId;
  const brand = getActiveBrand();
  if (!brand) {
    return;
  }

  setBrandWorkspaceEnabled(true);
  renderBrandList();
  populateBrandForm(brand);
  updateActiveBrandHeader(brand);

  const token = ++state.requestToken;
  try {
    await searchBrandProfile(brand, { brandId: brand.id, token });
    if (refresh) {
      statusText.textContent = "Kaynaklar arka planda taraniyor";
      setBusy(false, statusText.textContent);
      queueBrandRefresh(brand, brand.id, token);
    }
  } catch (error) {
    if (token === state.requestToken) {
      renderWarnings([error.message]);
      statusText.textContent = "Marka yuklenemedi";
      setBusy(false, statusText.textContent);
    }
  }
}

async function saveActiveBrandAndRefresh() {
  const brand = getActiveBrand();
  if (!brand) {
    statusText.textContent = "Once bir marka secin";
    return;
  }

  const profile = currentFormProfile();
  if (!profile.query_text) {
    statusText.textContent = "Filtre sorgusu zorunlu";
    return;
  }

  setBusy(true, "Filtre kaydediliyor");
  try {
    const response = await fetch(`/api/brands/${brand.id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: brand.name,
        query: profile.query_text,
        from: profile.requested_from,
        to: profile.requested_to,
        platforms: profile.platforms,
      }),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || "Filtre kaydedilemedi");
    }

    mergeBrand(data);
    updateActiveBrandHeader(getActiveBrand());
    await activateBrand(data.id, { refresh: true });
  } catch (error) {
    renderWarnings([error.message]);
    statusText.textContent = "Filtre kaydedilemedi";
    setBusy(false, statusText.textContent);
  }
}

async function createBrand() {
  const name = newBrandNameInput.value.trim();
  const query = newBrandQueryInput.value.trim();
  if (!name) {
    statusText.textContent = "Marka adi gerekli";
    return;
  }
  if (!query) {
    statusText.textContent = "Varsayilan filtre gerekli";
    return;
  }

  setBusy(true, "Marka olusturuluyor");
  try {
    const response = await fetch("/api/brands", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name,
        query,
        from: null,
        to: null,
        platforms: selectedPlatforms().length ? selectedPlatforms() : ["youtube"],
      }),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || "Marka eklenemedi");
    }

    brandFilterInput.value = "";
    newBrandNameInput.value = "";
    newBrandQueryInput.value = "";
    mergeBrand(data);
    state.activeBrandId = null;
    renderBrandList();
    populateBrandForm(null);
    updateActiveBrandHeader(null);
    setBrandWorkspaceEnabled(false);
    renderWelcomeState();
    renderWarnings([]);
    statusText.textContent = "Marka eklendi. Sol listeden secerek ekranini acabilirsiniz.";
    setBusy(false, statusText.textContent);
  } catch (error) {
    renderWarnings([error.message]);
    statusText.textContent = "Marka eklenemedi";
    setBusy(false, statusText.textContent);
  }
}

async function bootstrap() {
  try {
    await fetchBrands();
    updateActiveBrandHeader(null);
    populateBrandForm(null);
    setBrandWorkspaceEnabled(false);
    renderWelcomeState();
    statusText.textContent = "Marka secimi bekleniyor";
  } catch (error) {
    renderWarnings([error.message]);
    statusText.textContent = "Baslangic yuklemesi basarisiz";
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  await saveActiveBrandAndRefresh();
});

searchCacheButton.addEventListener("click", async () => {
  const brand = getActiveBrand();
  if (!brand) {
    statusText.textContent = "Once bir marka secin";
    return;
  }
  await activateBrand(brand.id, { refresh: false });
});

refreshBrandButton.addEventListener("click", async () => {
  const brand = getActiveBrand();
  if (!brand) {
    statusText.textContent = "Once bir marka secin";
    return;
  }
  await activateBrand(brand.id, { refresh: true });
});

brandCreateForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  await createBrand();
});

brandFilterInput.addEventListener("input", renderBrandList);

brandList.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-brand-id]");
  if (!button) {
    return;
  }
  const brandId = Number(button.dataset.brandId);
  if (!brandId) {
    return;
  }
  await activateBrand(brandId, { refresh: true });
});

resultsList.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-read-toggle]");
  if (!button) {
    return;
  }

  const itemId = Number(button.dataset.itemId);
  if (!itemId) {
    return;
  }

  const currentState = button.getAttribute("aria-pressed") === "true";
  try {
    const response = await fetch(`/api/items/${itemId}/read`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ is_read: !currentState }),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || "Okundu durumu kaydedilemedi");
    }

    const activeBrand = getActiveBrand();
    if (activeBrand) {
      await activateBrand(activeBrand.id, { refresh: false });
    }
  } catch (error) {
    renderWarnings([error.message]);
    statusText.textContent = "Okundu durumu kaydedilemedi";
  }
});

document.querySelectorAll("[data-range-days]").forEach((button) => {
  button.addEventListener("click", () => {
    applyRangeDays(Number(button.dataset.rangeDays));
  });
});

document.querySelectorAll("[data-range-clear]").forEach((button) => {
  button.addEventListener("click", () => {
    applyRangeDays(null);
  });
});

bootstrap();
