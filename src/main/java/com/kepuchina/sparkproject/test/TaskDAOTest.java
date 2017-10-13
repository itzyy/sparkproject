package com.kepuchina.sparkproject.test;

import com.kepuchina.sparkproject.dao.ITaskDao;
import com.kepuchina.sparkproject.dao.impl.DAOFactory;
import com.kepuchina.sparkproject.domain.Task;

/**
 * 任务管理DAO测试类
 */
public class TaskDAOTest {

    public static void main(String[] args) {
        ITaskDao taskDao = DAOFactory.getTaskDao();
        Task task = taskDao.findById(1);
        System.out.println(task.getTaskName());
    }
}
