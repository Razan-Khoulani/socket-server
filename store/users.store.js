// store/users.store.js
const userDetails = new Map();
const userDetailsByToken = new Map();

const normalizeToken = (token) => {
  if (token == null) return null;
  const t = String(token).trim();
  return t.length ? t : null;
};

const setUserDetails = (userId, details) => {
  if (!userId) return;
  const prev = userDetails.get(userId) || {};
  const merged = {
    ...prev,
    ...details,
    user_id: userId,
    updated_at: Date.now(),
  };
  userDetails.set(userId, merged);

  const token = normalizeToken(
    details?.user_token ?? details?.token ?? prev?.user_token ?? prev?.token
  );
  if (token) {
    userDetailsByToken.set(token, merged);
  }
};

const getUserDetails = (userId) => {
  if (!userId) return null;
  return userDetails.get(userId) || null;
};

const getUserDetailsByToken = (token) => {
  const t = normalizeToken(token);
  if (!t) return null;
  return userDetailsByToken.get(t) || null;
};

const deleteUserDetails = (userId) => {
  if (!userId) return;
  userDetails.delete(userId);
};

module.exports = {
  userDetails,
  setUserDetails,
  getUserDetails,
  getUserDetailsByToken,
  deleteUserDetails,
};
