import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'package:pointycastle/export.dart' as pc;

class ZeytinCipher {
  final Uint8List _key;
  final Random _random = Random.secure();

  ZeytinCipher(String keyString)
      : _key = Uint8List.fromList(
          utf8.encode(_padOrTruncateKey(keyString)),
        );

  static String _padOrTruncateKey(String key) {
    if (key.length == 32) return key;
    if (key.length > 32) return key.substring(0, 32);
    return key.padRight(32, '0');
  }

  pc.PaddedBlockCipher _createCipher(
    bool forEncryption,
    Uint8List iv,
  ) {
    final cipher = pc.PaddedBlockCipher('AES/CBC/PKCS7');

    cipher.init(
      forEncryption,
      pc.PaddedBlockCipherParameters<
          pc.ParametersWithIV<pc.KeyParameter>, Null>(
        pc.ParametersWithIV<pc.KeyParameter>(
          pc.KeyParameter(_key),
          iv,
        ),
        null,
      ),
    );

    return cipher;
  }

  Uint8List encode(Uint8List plainText) {
    final iv = Uint8List.fromList(
      List<int>.generate(16, (_) => _random.nextInt(256)),
    );

    final encrypted = _createCipher(true, iv).process(plainText);

    final builder = BytesBuilder();
    builder.add(iv);
    builder.add(encrypted);

    return builder.toBytes();
  }

  Uint8List decode(Uint8List cipherTextWithIv) {
    if (cipherTextWithIv.length < 32) {
      throw const FormatException(
        'ZeytinCipher Hatası: Şifreli veri çok kısa veya bozuk.',
      );
    }

    if ((cipherTextWithIv.length - 16) % 16 != 0) {
      throw const FormatException(
        'ZeytinCipher Hatası: Şifreli veri uzunluğu geçersiz.',
      );
    }

    final iv = cipherTextWithIv.sublist(0, 16);
    final actualCipherText = cipherTextWithIv.sublist(16);

    return _createCipher(false, iv).process(actualCipherText);
  }
}