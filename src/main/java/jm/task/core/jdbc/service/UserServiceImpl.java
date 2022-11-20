package jm.task.core.jdbc.service;

import jm.task.core.jdbc.dao.UserDao;
import jm.task.core.jdbc.dao.UserDaoJDBCImpl;
import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import javax.persistence.Id;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserServiceImpl implements UserService {


    UserDaoJDBCImpl userDao = new UserDaoJDBCImpl();

    public UserServiceImpl() throws SQLException {
    }

    @Override
    public void createUsersTable() throws ClassNotFoundException, SQLException {
        userDao.createUsersTable();

    }

    @Override
    public void dropUsersTable() throws SQLException {
        userDao.dropUsersTable();

    }

    @Override
    public void saveUser(String name, String lastName, byte age) throws ClassNotFoundException, SQLException {
        userDao.saveUser(name, lastName, age);

    }

    @Override
    public void removeUserById(long id) throws SQLException {
        userDao.removeUserById(id);

    }

    @Override
    public List<User> getAllUsers() {
        try {
            return userDao.getAllUsers();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanUsersTable() throws SQLException {
        userDao.cleanUsersTable();

    }
}
