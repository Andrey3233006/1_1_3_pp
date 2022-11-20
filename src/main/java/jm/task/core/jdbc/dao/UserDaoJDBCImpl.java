package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.sql.ResultSet;
public class UserDaoJDBCImpl extends Util implements UserDao {


//    Создание таблицы для User(ов) – не должно приводить к исключению, если такая таблица уже существует
//    Удаление таблицы User(ов) – не должно приводить к исключению, если таблицы не существует
//    Очистка содержания таблицы
//    Добавление User в таблицу
//    Удаление User из таблицы ( по id )
//    Получение всех User(ов) из таблицы



    Connection connection = getConnection();

    public UserDaoJDBCImpl() throws SQLException {
    }


    @Override
    public void createUsersTable() throws ClassNotFoundException, SQLException {
        PreparedStatement preparedStatement = null;

        String createSql = "CREATE TABLE IF NOT EXISTS users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR (50) NOT NULL, lastName VARCHAR (50) NOT NULL, age TINYINT NOT NULL)";

        try {
            preparedStatement = Util.getConnection().prepareStatement(createSql);
            preparedStatement.execute(createSql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    @Override
    public void dropUsersTable() throws SQLException {
        Statement statement = null;

        String dropSql = "DROP TABLE IF EXISTS users";
        try {
            statement = connection.prepareStatement(dropSql);
            statement.execute(dropSql);
            connection.commit();


        } catch (SQLException e){

            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }


    @Override
    public void saveUser(String name, String lastName, byte age) throws SQLException {
        PreparedStatement preparedStatement = null;

        String saveSql = "INSERT INTO users ( NAME, LASTNAME, AGE) VALUES ( ?, ?, ?)";

        try {
            preparedStatement = connection.prepareStatement(saveSql);

            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);

            preparedStatement.executeUpdate();
            System.out.println("User "+ name + " успешно добавлен в базу данных.");

            connection.commit();


        } catch (SQLException e){

            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void removeUserById ( long id) throws SQLException {

            PreparedStatement preparedStatement = null;

            String removeSql = "DELETE FROM users WHERE ID = ?";


            try {
                preparedStatement = connection.prepareStatement(removeSql);

                preparedStatement.setLong(1,id);
                preparedStatement.executeUpdate();
                connection.commit();


            } catch (SQLException e){

                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }

            }
    }

        @Override
        public List<User> getAllUsers () throws SQLException {
            List<User> userList = new ArrayList<>();
            String getAllSql = "SELECT id, name, lastname, age FROM users";
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = connection.prepareStatement(getAllSql);
                ResultSet resultSet = preparedStatement.executeQuery(getAllSql);
                while (resultSet.next()) {

                    User users = new User();
                    users.setId(resultSet.getLong("id"));
                    users.setName(resultSet.getString("name"));
                    users.setLastName(resultSet.getString("lastName"));
                    users.setAge( resultSet.getByte("age"));

                    userList.add(users);
                }

                connection.commit();


            } catch (SQLException e){

                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }

            }
            System.out.println(userList);
            return userList;
        }
    @Override
    public void cleanUsersTable () throws SQLException {
        Statement statement = null;
        String cleanSql = "TRUNCATE users";
        try {
            statement = connection.prepareStatement(cleanSql);
            statement.executeUpdate(cleanSql);

            connection.commit();

        } catch (SQLException e){

            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }

        }
    }
}

