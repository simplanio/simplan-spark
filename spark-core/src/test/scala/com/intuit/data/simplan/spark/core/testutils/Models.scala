/*
*  Copyright 2025, Intuit Inc
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*         http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
package com.intuit.data.simplan.spark.core.testutils

/**
 * @author Abraham, Thomas - tabraham1
 *         Created on 07-Mar-2025 at 10:23 AM
 */
case class ContactDetailsModel(email: String, address: List[AddressModel], phoneNumbers: Array[String], `primary address`: AddressModel, `primary number`: String)

case class AddressModel(street: String, `zip code`: Int)

case class UsersModel(name: String, age: Int, contact_details: ContactDetailsModel)

object UsersModel {
  def apply(name: String, age: Int, street: String): UsersModel = UsersModel(
    name = name,
    age = age,
    contact_details = ContactDetailsModel(
      email = name + "@example.com",
      address = (1 to 5).map(each => AddressModel(street + s"_$each", age + 94000 + each)).toList,
      phoneNumbers = Array(
        (4088760000L + age).toString,
        (6692455586L + age).toString
      ),
      `primary address` = AddressModel(street, age + 94000),
      `primary number` = (4088760000L + age).toString
    ))
}


