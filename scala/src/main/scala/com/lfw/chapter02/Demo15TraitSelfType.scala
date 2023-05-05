package com.lfw.chapter02

object Demo15TraitSelfType {
  def main(args: Array[String]): Unit = {
    val user = new RegisterUser("alice", "123456")
    user.insert() //insert into db: alice
  }
}

//用户类
class User(val name: String, val password: String)

trait UserDao {
  _: User => //相当于引入一个特质，类型为 User，可以通过 this. 调用相关属性
  //向数据库插入数据
  def insert(): Unit = {
    println(s"insert into db: ${this.name}")  // this.name 调用的是 User 的 name 属性
  }
}

//定义注册用户类
class RegisterUser(name: String, password: String) extends User(name, password) with UserDao
