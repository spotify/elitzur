# Elitzur Data Validation

## Overview

This library allows you to:

* use custom Scala types in your Scio data pipeline
* perform validation on these types
* [filter or transform invalid data](#custom-validation-behaviour)
* use Beam counters to check validity
* convert Avro records into custom-typed case classes

This README will show you how to incorporate this library into your Scio data pipeline.

#### What does Elitzur mean?
The [Elitzur-Vaidman bomb tester](https://en.wikipedia.org/wiki/Elitzur%E2%80%93Vaidman_bomb_tester)
is a thought experiment that verifies a bomb is functional without having to detonate it.

## How to use in your data pipeline

### Step by step integration guide

1. You must be using Scio version `0.8.1` or greater.

2. Add these libraries in your `build.sbt` library dependencies

    You'll always want to add:

    ```sbtshell
    "com.spotify" %% "elitzur-core" % "CURRENT_VERSION",
    "com.spotify" %% "elitzur-scio" % "CURRENT_VERSION"
    ```

    If you're using Avro case class conversions you'll additionally want:

      ```sbtshell
        "com.spotify" %% "elitzur-avro" % "CURRENT_VERSION"
      ```

    The current version is available in Releases tab on Github.

    You will also need to create appropriate validation types for your data. 
    
3. Follow instructions below for Avro if you want automatic conversions. However, you can also use Elitzur with any hand crafted case class simply by changing the types of the fields you want to validate to their corresponding Validation Types.

    For example:

    ```scala
        case class User(userId: String, country: String, age: Long)
        val user = User("test", "US", 25L)
    ```
    would become:

    ```scala
        case class User(userId: UserId, country: CountryCode, age: Age)
        val user = User(UserId("test"), CountryCode("US"), Age(25L))
    ```

4. Call `.validate()` on your SCollection.  This will validate all of the ValidationTypes in
   your data and complete the actions specified by your configuration.


#### Avro
   1. Manually define case classes containing only the fields you care about for your pipeline replacing
   relevant fields with Validation Type classes.

   For example, given the following avro schema:

   ```
           {
             "name": "MyRecordAvro",
             "namespace": "com.spotify.skeleton.schema",
             "type": "record",
             "fields": [
               {
                 "name": "userAge",
                 "type": "long",
                 "doc": "{validationType: age}"
               },
               {
                 "name": "userFloat",
                 "type": "float",
                 "doc": "floating"
               },
               {
                 "name": "userDouble",
                 "type": "double",
                 "doc": "{validationType: nonNegativeDouble}"
               },
               {
                 "name": "userLong",
                 "type": "long",
                 "doc": "{validationType: nonNegativeLong}"
               },
               {
                 "name": "inner",
                 "type": "com.spotify.skeleton.schema.InnerNestedType"
               },
             ]
           }
   ```

   We could have a case class for a pipeline that only uses `userAge`, `userFloat`, and `userDouble`
   defined:

   ```scala
   case class MyRecord(userAge: Age, userFloat: Float, userDouble: NonNegativeDouble)
   ```

   Note that if you have a field that is nullable in your Avro schema (i.e. if your field is defined in Avro as a union with null), you'll want to wrap that type in an `Option` in your case class.

   2. In order convert to/from Avro you must import our converters. You'll also need the same validators import as BigQuery.

   ```scala
   import com.spotify.elitzur.converters.avro._
   import com.spotify.elitzur.validators._
   import com.spotify.elitzur.scio._
   ```

  These imports contain implicit values which need to be pulled into scope. IntelliJ may mark the imports as unused, but if you don't have them, compilation will fail with a "could not find implicit value" message.

   3. Call fromAvro on your SCollection, providing the target type

   ```scala
   val coll: SCollection[MyRecordAvro] = ???
   coll.fromAvro[MyRecord]
   ```

   4. After validation or when outputting, you can convert to an Avro record using toAvro
   ```scala
   val coll: SCollection[MyRecord] = ???
   coll.toAvro[OutputAvro]
   ```

### Specific Field Types

#### Using `Option` and `NULLABLE`

When using fields `Option` or type `NULLABLE` it is important to note that when the value is `None`
or missing this will count as **Valid** for now. 

#### Using `List` and `REPEATED`

This is supported, **but** requires iteration of each List independently. It is up to the user to
 determine whether this is more efficient than flattening prior to validation, since this can vary on a case-by-case basis.

#### Complex Avro Types

Complex avro types are currently not supported. This includes Unions (except those used for nullable fields),
 Fixed, and Map.

#### Avro Enums

Elitzur supports conversion of Avro enums to enums represented with the scala library `Enumeratum`.
In order to convert an avro enum simply use a matching Enumeratum enum as the type for the corresponding field in your case class.
You can read more about Enumeratum [here](https://github.com/lloydmeta/enumeratum).

## Additional configuration

If you would like to customize what happens when a validation rule is violated you can pass in a
`ValidationRecordConfig` when you call `validate`

So for example this will throw an exception whenever it sees an invalid country code and won't log
any counters for the age field.

```scala
pipe.validate(ValidationRecordConfig("country" -> ThrowException, "age" -> NoCounter))
```

For nested fields simply separate fields names with `.`

```scala
pipe.validate(ValidationRecordConfig("nested.country" -> ThrowException, "nested.age" -> NoCounter))
```

The available configurations are:

* `ThrowException` controls if an exception is thrown on violation of rules. By default no exception
is thrown. If you'd like to override this add your field to this annotation above the class or case
class
    * Example: `ValidationRecordConfig("country" -> ThrowException)`
* `NoCounter` controls if a Beam counter is logged on violation of rules, by default a counter is
recorded, if you'd like to override this add your field to this annotation above the class or case
class
    * Example: ValidationRecordConfig("age" -> NoCounter)


#### Custom Validation Behaviour
When you create your own case classes you have the option to wrap Validation Types in a `ValidationStatus`
type that will let you access the validation status of that field (Valid/Invalid) in code.
For example:
a `ValidationStatus[CountryCode]` will be either `Valid(CountryCode("US"))` or `Invalid(CountryCode("USA"))`
after performing validation.
If these are wrapped in ValidationStatus, they will always come out of `validate()` wrapped in Valid/Invalid. You can also wrap an entire record in Valid/Invalid (it's invalid if at least one field is invalid) by calling `validateWithResult()` on your to-be-validated SCollection.

This allows you to match on the Validation and respond to invalid results however you see fit.

Case classes or fields wrapped in the `Invalid` ValidationStatus will be filtered out if you call `flatten` or `flatMap` on a collection or SCollection of them.

PLEASE NOTE this does have performance costs so it should only be used when additional customization
is necessary.  This will increase your data size and slow down shuffles.

Here is a more complete example:
```scala
case class Record(userId: UserId, country: ValidationStatus[CountryCode], age: ValidationStatus[Age])
val pipe: SCollection[Record] = ???

pipe.validate()
    .filter(_.country.isValid)
    .map(r => r.age match {
      case Valid(age) => age.data
      case Invalid(_) => 0L
    })
```
the above code will filter all Records with invalid countries and return the age where invalid ages
are replaced with zero.

An example of filtering out Invalid results could look like:
```scala
case class Record(userId: UserId, country: CountryCode, age: Age)
val pipe: SCollection[Record] = ???

pipe.validateWithResult().flatten // contains only Valid values
```

When constructing case class containing these fields you wrap the Validation Type classes in
`Unvalidated`.

```scala
val r = Record(UserId("test"), Unvalidated(CountryCode("US")), Unvalidated(Age(25L)))
```

You can also use this on nested records.  It will be Invalid if any Validation Type fields in the
nested record are invalid.  Otherwise it will be valid.


## Testing Avro pipelines with toAvroDefault
In your pipeline code you should always use `toAvro` if you want to convert a case class to an Avro record, as it's performance optimized and when you're writing a pipeline you have control of your output schemas. `toAvro` will fail if an Avro record has a required field that is not specified in the case class you're converting to Avro record. This is a user code error - your pipeline should write an output Avro schema that does not contain required fields that aren't in the case class you're converting.

If you're writing a unit test and don't have control of the input schema, you may want to use `toAvroDefault` to do an Avro conversion to generate input data from a case class. Note that we do not make performance guarantees around `toAvroDefault`, so it should be used only in tests and not in production workflows.

The test workflow using `toAvroDefault` works like this:

1. Generate case classes containing the fields you care about (this can be done in several ways)
2. Generate a SpecificRecord of your input type with values in all required fields (this can be done with `specificRecordOf` in [Ratatool-Scalacheck](https://github.com/spotify/ratatool/tree/master/ratatool-scalacheck#usage))
3. Use both the case classes you've generated and the SpecificRecord as arguments to `toAvroDefault`
4. The output of `toAvroDefault` for a case class will be a SpecificRecord where all fields in your case class are copied into the record, and all fields not specified in the case class will have the values given in your "default" record. Repeated nested records will take their default values from the first nested record in the repeated list.

