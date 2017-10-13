package com.kepuchina.sparkproject.dao.impl;

import com.kepuchina.sparkproject.dao.ITaskDao;
import com.kepuchina.sparkproject.domain.Task;
import com.kepuchina.sparkproject.jdbc.JdbcHelper;

import java.sql.ResultSet;

/**
 * Created by Zouyy on 2017/9/28.
 */
public class TaskDaoImpl implements ITaskDao {
    /**
     * 根据主键查询任务
     * @param taskid
     * @return
     */
    public Task findById(long taskid) {
        final Task task = new Task();

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        String sql="select * from task where taskid=?";
        Object[] params = new Object[]{taskid};
        jdbcHelper.executeQuery(sql, params, new JdbcHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if(rs.next()) {
                    long taskId = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskid(taskId);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });


        return task;
    }
}
