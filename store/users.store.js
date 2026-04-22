// store/users.store.js
const userDetails = new Map();
const userDetailsByToken = new Map();
const activeUserToken = new Map(); // userId -> latest token

const normalizeToken = (token) => {
  if (token == null) return null;
  const t = String(token).trim();
  return t.length ? t : null;
};

const setUserDetails = (userId, details) => {
  if (!userId) return;
  const safeUserId = Number(userId);
  if (!Number.isFinite(safeUserId)) return;

  const previous = userDetails.get(safeUserId) || {};
  const previousToken = normalizeToken(
    activeUserToken.get(safeUserId) ??
      previous?.user_token ??
      previous?.token ??
      previous?.access_token ??
      null
  );

  const incomingToken = normalizeToken(
    details?.user_token ?? details?.token ?? details?.access_token ?? null
  );
  const resolvedToken = incomingToken ?? previousToken ?? null;

  // Remove stale token index for this user when token rotates.
  if (previousToken && previousToken !== resolvedToken) {
    const mapped = userDetailsByToken.get(previousToken);
    if (mapped?.user_id === safeUserId) {
      userDetailsByToken.delete(previousToken);
    }
  }

  const prev = userDetails.get(safeUserId) || {};
  const merged = {
    ...prev,
    ...details,
    user_id: safeUserId,
    ...(resolvedToken
      ? {
          user_token: resolvedToken,
          token: resolvedToken,
          access_token: resolvedToken,
        }
      : {}),
    updated_at: Date.now(),
  };
  userDetails.set(safeUserId, merged);

  if (resolvedToken) {
    userDetailsByToken.set(resolvedToken, merged);
    activeUserToken.set(safeUserId, resolvedToken);
  }
};

const getUserDetails = (userId) => {
  if (!userId) return null;
  const safeUserId = Number(userId);
  if (!Number.isFinite(safeUserId)) return null;
  return userDetails.get(safeUserId) || null;
};

const getUserDetailsByToken = (token) => {
  const t = normalizeToken(token);
  if (!t) return null;
  return userDetailsByToken.get(t) || null;
};

const deleteUserDetails = (userId) => {
  if (!userId) return;
  const safeUserId = Number(userId);
  if (!Number.isFinite(safeUserId)) return;

  const current = userDetails.get(safeUserId) || null;
  const token = normalizeToken(
    activeUserToken.get(safeUserId) ??
      current?.user_token ??
      current?.token ??
      current?.access_token ??
      null
  );
  if (token) {
    const mapped = userDetailsByToken.get(token);
    if (mapped?.user_id === safeUserId) {
      userDetailsByToken.delete(token);
    }
  }
  activeUserToken.delete(safeUserId);

  // Safety cleanup for any stale token entries that still point to this user.
  for (const [tokenKey, mapped] of userDetailsByToken.entries()) {
    if (mapped?.user_id === safeUserId) {
      userDetailsByToken.delete(tokenKey);
    }
  }

  userDetails.delete(safeUserId);
};

module.exports = {
  userDetails,
  setUserDetails,
  getUserDetails,
  getUserDetailsByToken,
  deleteUserDetails,
};
