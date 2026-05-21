const trimBaseUrl = (value) => String(value || "").trim().replace(/\/+$/, "");

const normalizePublicAssetUrl = (
  value,
  {
    baseUrl = "",
    defaultRelativeDir = "",
    emptyValue = "",
    upgradeSameHostToHttps = true,
  } = {}
) => {
  const raw = typeof value === "string" ? value.trim() : "";
  if (!raw) return emptyValue;

  if (/^data:/i.test(raw)) return raw;

  const base = trimBaseUrl(baseUrl);

  if (/^https?:\/\//i.test(raw)) {
    if (upgradeSameHostToHttps && base.startsWith("https://") && raw.startsWith("http://")) {
      try {
        const baseObj = new URL(base);
        const imageObj = new URL(raw);
        if (imageObj.hostname === baseObj.hostname) {
          imageObj.protocol = "https:";
          return imageObj.toString();
        }
      } catch (_) {}
    }
    return raw;
  }

  if (raw.startsWith("//")) {
    const protocol = base.startsWith("https://") ? "https:" : "http:";
    return `${protocol}${raw}`;
  }

  const cleaned = raw.replace(/\\/g, "/").replace(/^\.\/+/, "");

  if (cleaned.startsWith("/assets/")) {
    return base ? `${base}${cleaned}` : cleaned;
  }

  const assetsIndex = cleaned.indexOf("assets/");
  if (assetsIndex >= 0) {
    const relative = cleaned.slice(assetsIndex).replace(/^\/+/, "");
    return base ? `${base}/${relative}` : `/${relative}`;
  }

  if (!cleaned.includes("/")) {
    if (!defaultRelativeDir) {
      return base ? `${base}/${cleaned}` : cleaned;
    }
    return base ? `${base}/${defaultRelativeDir}/${cleaned}` : `${defaultRelativeDir}/${cleaned}`;
  }

  const relative = cleaned.replace(/^\/+/, "");
  return base ? `${base}/${relative}` : `/${relative}`;
};

module.exports = {
  normalizePublicAssetUrl,
};

