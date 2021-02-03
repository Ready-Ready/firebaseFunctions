module.exports = {
  root: true,
  env: {
    es6: false,
    node: true,
  },
  extends: [
    "eslint:recommended",
    "google",
  ],
  rules: {
    quotes: ["error", "double"],
  },
};
