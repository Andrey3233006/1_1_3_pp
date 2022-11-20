package jm.task.core.jdbc;

import jm.task.core.jdbc.dao.UserDao;
import jm.task.core.jdbc.dao.UserDaoJDBCImpl;
import java.sql.SQLException;

public class Main  {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

//    В методе main класса Main должны происходить следующие операции:
//
//        Создание таблицы User(ов)
//        Добавление 4 User(ов) в таблицу с данными на свой выбор. После каждого добавления должен быть вывод в консоль ( User с именем – name добавлен в базу данных )
//        Получение всех User из базы и вывод в консоль ( должен быть переопределен toString в классе User)
//        Очистка таблицы User(ов)
//        Удаление таблицы


        UserDao userDao = new UserDaoJDBCImpl();

        userDao.createUsersTable();

        userDao.saveUser("Name2", "LastName2", (byte) 22);
        userDao.saveUser("Name3", "LastName3", (byte) 23);
        userDao.saveUser("Name4", "LastName4", (byte) 24);
        userDao.saveUser("Name1", "LastName1", (byte) 21);

        userDao.removeUserById(2);

        userDao.getAllUsers();

        userDao.cleanUsersTable();

        userDao.dropUsersTable();
    }
}


