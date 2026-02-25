import 'dart:async';
import 'package:zeytin_local_storage/zeytin_local_storage.dart';
import 'package:test/test.dart';

void main() {
  group('ZeytinStorage Core Operations', () {
    late ZeytinStorage zeytin;
    final String testBox = 'test_box';

    setUpAll(() async {
      // Initialize Zeytin once before all tests in this group run
      zeytin = ZeytinStorage(
        namespace: "test_namespace",
        truckID: "test_truck",
        encrypter: ZeytinCipher("my_super_secret_password_123456"),
      );
      zeytin.initialize("./zeytin");

      // Give the isolate a brief moment to spin up
      await Future.delayed(const Duration(milliseconds: 500));
    });

    tearDownAll(() async {
      // Clean up the database after all tests are done
      final completer = Completer<void>();
      await zeytin.deleteAll(
        onSuccess: () => completer.complete(),
        onError: (e, s) => completer.completeError(e),
      );
      await completer.future;
    });

    test('Add and Read Normal Data', () async {
      final completer = Completer<Map<String, dynamic>?>();

      // 1. Add Data
      await zeytin.add(
        data: ZeytinValue(testBox, 'user_1', {'name': 'Alice', 'age': 25}),
        onSuccess: () async {
          // 2. Read Data
          await zeytin.get(
            boxId: testBox,
            tag: 'user_1',
            onSuccess: (result) => completer.complete(result.value),
            onError: (e, s) => completer.completeError(e),
          );
        },
        onError: (e, s) => completer.completeError(e),
      );

      final data = await completer.future;
      expect(data, isNotNull);
      expect(data!['name'], equals('Alice'));
      expect(data['age'], equals(25));
    });

    test('Add and Read Encrypted Data', () async {
      final completer = Completer<Map<String, dynamic>?>();

      await zeytin.add(
        data: ZeytinValue(testBox, 'secret_user', {
          'secret': 'Top Secret Data',
        }),
        isEncrypt: true,
        onSuccess: () async {
          await zeytin.get(
            boxId: testBox,
            tag: 'secret_user',
            onSuccess: (result) => completer.complete(result.value),
            onError: (e, s) => completer.completeError(e),
          );
        },
        onError: (e, s) => completer.completeError(e),
      );

      final data = await completer.future;
      expect(data, isNotNull);
      expect(data!['secret'], equals('Top Secret Data'));
    });

    test('Search Data by Prefix', () async {
      final completer = Completer<List<ZeytinValue>>();

      // Add a specific user for search
      await zeytin.add(
        data: ZeytinValue(testBox, 'search_target', {
          'username': 'ZeytinAdmin',
        }),
        onSuccess: () async {
          await zeytin.search(
            boxId: testBox,
            field: 'username',
            prefix: 'Zeytin',
            onSuccess: (results) => completer.complete(results),
            onError: (e, s) => completer.completeError(e),
          );
        },
        onError: (e, s) => completer.completeError(e),
      );

      final results = await completer.future;
      expect(results.isNotEmpty, isTrue);
      expect(results.first.value?['username'], equals('ZeytinAdmin'));
    });

    test('Delete Data', () async {
      final deleteCompleter = Completer<void>();
      final checkCompleter = Completer<bool>();

      // 1. Remove the data
      await zeytin.remove(
        boxId: testBox,
        tag: 'user_1',
        onSuccess: () => deleteCompleter.complete(),
        onError: (e, s) => deleteCompleter.completeError(e),
      );
      await deleteCompleter.future;

      // 2. Verify it's gone
      await zeytin.existsTag(
        boxId: testBox,
        tag: 'user_1',
        onSuccess: (exists) => checkCompleter.complete(exists),
        onError: (e, s) => checkCompleter.completeError(e),
      );

      final exists = await checkCompleter.future;
      expect(exists, isFalse);
    });
  });

  group('ZeytinMini Operations', () {
    setUpAll(() async {
      // Initialize ZeytinMini
      await ZeytinMini.init("./zeytin");
    });

    tearDownAll(() async {
      await ZeytinMini.clear();
    });

    test('Mini Add and Get Data', () async {
      await ZeytinMini.add('config', {'theme': 'dark', 'version': 1.0});

      final data = await ZeytinMini.get('config');

      expect(data, isNotNull);
      expect(data!['theme'], equals('dark'));
      expect(data['version'], equals(1.0));
    });

    test('Mini Contains and Remove Data', () async {
      await ZeytinMini.add('temp_key', {'val': 123});

      bool exists = await ZeytinMini.contains('temp_key');
      expect(exists, isTrue);

      await ZeytinMini.remove('temp_key');

      exists = await ZeytinMini.contains('temp_key');
      expect(exists, isFalse);
    });

    test('Mini Get All Keys and Values', () async {
      await ZeytinMini.add('key_1', {'val': 1});
      await ZeytinMini.add('key_2', {'val': 2});

      final keys = await ZeytinMini.getAllKeys();
      final values = await ZeytinMini.getAllValues();

      expect(keys, containsAll(['key_1', 'key_2']));
      expect(values.length, greaterThanOrEqualTo(2));
    });
  });
}
