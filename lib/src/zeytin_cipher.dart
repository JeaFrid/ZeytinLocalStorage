import 'dart:typed_data';
import 'package:encrypt/encrypt.dart' as encrypt;

class ZeytinCipher {
  final encrypt.Key _key;
  late final encrypt.Encrypter _encrypter;
  ZeytinCipher(String keyString)
    : _key = encrypt.Key.fromUtf8(_padOrTruncateKey(keyString)) {
    _encrypter = encrypt.Encrypter(
      encrypt.AES(_key, mode: encrypt.AESMode.cbc, padding: 'PKCS7'),
    );
  }
  static String _padOrTruncateKey(String key) {
    if (key.length == 32) return key;
    if (key.length > 32) return key.substring(0, 32);
    return key.padRight(32, '0');
  }
  Uint8List encode(Uint8List plainText) {
    final iv = encrypt.IV.fromSecureRandom(16);
    final encrypted = _encrypter.encryptBytes(plainText.toList(), iv: iv);
    final builder = BytesBuilder();
    builder.add(iv.bytes);
    builder.add(encrypted.bytes);

    return builder.toBytes();
  }
  Uint8List decode(Uint8List cipherTextWithIv) {
    if (cipherTextWithIv.length < 16) {
      throw Exception('ZeytinCipher Hatası: Şifreli veri çok kısa veya bozuk.');
    }
    final ivBytes = cipherTextWithIv.sublist(0, 16);
    final actualCipherText = cipherTextWithIv.sublist(16);
    final iv = encrypt.IV(ivBytes);
    final encryptedData = encrypt.Encrypted(actualCipherText);
    final decryptedList = _encrypter.decryptBytes(encryptedData, iv: iv);

    return Uint8List.fromList(decryptedList);
  }
}
