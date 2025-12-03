import { Platform } from 'react-native';
export const isWeb = () => {
  return Platform.OS === 'web';
};
export const isNative = () => {
  return !isWeb();
};
//# sourceMappingURL=platform.js.map