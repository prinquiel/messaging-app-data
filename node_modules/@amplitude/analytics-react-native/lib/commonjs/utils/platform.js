"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isWeb = exports.isNative = void 0;
var _reactNative = require("react-native");
const isWeb = () => {
  return _reactNative.Platform.OS === 'web';
};
exports.isWeb = isWeb;
const isNative = () => {
  return !isWeb();
};
exports.isNative = isNative;
//# sourceMappingURL=platform.js.map