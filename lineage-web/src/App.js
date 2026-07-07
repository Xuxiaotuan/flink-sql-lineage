
import React,{ useEffect, useState } from 'react'
import { Outlet, Link, useLocation} from 'react-router-dom'
import { DownOutlined, ProfileOutlined, LoginOutlined, DatabaseOutlined, ToolOutlined, UserOutlined, TeamOutlined, ExclamationCircleFilled, GithubOutlined, BranchesOutlined, SettingOutlined } from '@ant-design/icons';
import { ConfigProvider, Layout, Menu, theme, Modal, Tooltip, Dropdown, Space, message } from 'antd'
import './common/common.styl'
import axios from 'axios'
import Logo from './page-login/img/logo-white.png'

const { Header, Content, Footer, Sider } = Layout
const { confirm } = Modal
const appTheme = {
  algorithm: theme.darkAlgorithm,
  token: {
    colorPrimary: '#00d992',
    colorInfo: '#2fd6a1',
    colorSuccess: '#00d992',
    colorWarning: '#f59e0b',
    colorError: '#ef4444',
    colorBgBase: '#101010',
    colorBgLayout: '#101010',
    colorBgContainer: '#1a1a1a',
    colorBgElevated: '#202020',
    colorBorder: '#333634',
    colorBorderSecondary: '#262826',
    colorText: '#f2f2f2',
    colorTextSecondary: '#bdbdbd',
    colorTextTertiary: '#8b949e',
    borderRadius: 6,
    borderRadiusLG: 8,
    fontFamily: 'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
  },
  components: {
    Layout: {
      bodyBg: '#101010',
      headerBg: '#101010',
      siderBg: '#101010',
      footerBg: '#101010',
    },
    Menu: {
      darkItemBg: '#101010',
      darkSubMenuItemBg: '#101010',
      darkItemColor: '#bdbdbd',
      darkItemHoverColor: '#f2f2f2',
      darkItemSelectedBg: 'rgba(0, 217, 146, 0.12)',
      darkItemSelectedColor: '#00d992',
      itemBorderRadius: 6,
    },
    Button: {
      colorPrimary: '#00d992',
      colorPrimaryHover: '#2fd6a1',
      colorPrimaryActive: '#10b981',
      primaryColor: '#101010',
    },
  },
}

const App = () => {
  const [collapsed, setCollapsed] = useState(false)
  const location = useLocation()
  const [current, setCurrent] = useState(location.pathname.split('/')[1])
  const userInfo = JSON.parse(window.localStorage.getItem('userInfo'))
  const {username=''} = userInfo || {}

  const menuMap = [
    {
      key: 'job',
      icon: BranchesOutlined,
      label: 'Job',
      url: 'job',
    },
    // {
    //   key: 'catalog',
    //   icon: FileTextOutlined,
    //   label: 'Catalog',
    //   url: 'catalog',
    // },
    {
      key: 'catalog',
      icon: DatabaseOutlined,
      label: 'Catalog',
      url: 'catalog',
    },
    {
      key: 'plugin',
      icon: ToolOutlined,
      label: 'Plugin',
      url: 'plugin',
    },
    {
      key: 'user-manage',
      icon: TeamOutlined,
      label: 'User',
      url: 'user-manage',
    },
  ]

  const onClickMenu = (e) => {
    console.log('click ', e);
    setCurrent(e.key);
  }
  const signOut = () => {
    confirm({
      title: 'Do you Want to Sign out?',
      icon: <ExclamationCircleFilled />,
      okText: 'Yes',
      cancelText: 'No',
      onOk: async () => {
        try {
          const res = await axios.post('/logout')
          const login_status = window.localStorage.getItem('login_status')
          if (res.data.data && login_status == 1) {
            window.localStorage.setItem('login_status', 0)
            window.location.href = '/#/login'
          }
        } catch (error) {
          message.error(error)
        }
      },
      onCancel() {
        console.log('Cancel');
      },
    });
  }

  const items = [
    {
      key: '1',
      label: (
        <div onClick={signOut}>
          <LoginOutlined className='mr16' />
          Sign out
        </div>
      ),
    },
  ]

  useEffect(() => {
    const login_status = window.localStorage.getItem('login_status')
    if (login_status == 0) {
      window.location.href = '/#/login'
    }
  }, [])

  useEffect(() => {
    setCurrent(location.pathname.split('/')[1])
  }, [location.pathname])
  
  return (
    <ConfigProvider theme={appTheme}>
    <Layout className="lineage-app-shell">
      <Sider
        breakpoint="lg"
        width={220}
        className="lineage-sider"
      >
        <div className="logo">
          <img src={Logo} width={40} height={40} />
          <span className='logo-txt fs16 bold-600'>FlinkSQL Lineage</span>
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[current]}
          onClick={onClickMenu}
          items={menuMap.map(
            (item) => ({
              key: item.key,
              icon: React.createElement(item.icon),
              label: <Link 
              to={`/${item.url}`}
            >{item.label}</Link>,
            }),
          )
        }
        />
      </Sider>
      <Layout>
        <Header
          className="FBV FBAE header-box lineage-header"
        >
          <div className='lineage-header-actions'>
            <Tooltip title='Document'>
              <ProfileOutlined className='lineage-header-icon hand' />
            </Tooltip>
            <Tooltip title='GitHub'>
              <Link to='https://github.com/HamaWhiteGG/flink-sql-lineage' target='_blank'>
                <GithubOutlined className='lineage-header-icon hand' />
              </Link>
            </Tooltip>
            {/* <Button type='link'>EN</Button> */}
            <Dropdown 
              menu={{
                items,
              }}
              trigger='click'
            >
              <Space className="lineage-user-menu">
                <span className='hand'>{username}</span>
                <DownOutlined />
              </Space>
            </Dropdown>
          </div>
        </Header>
        <Content
          className="lineage-content"
          style={{
            height: 'calc(100vh - 115px)',
            overflow: 'scroll',
          }}
        >
          <div
            className="lineage-content-inner"
            style={{
              minHeight: 360,
              height: '100%'
            }}
          >
            <Outlet />
          </div>
        </Content>
        <Footer
          className="lineage-footer"
          style={{
            textAlign: 'center',
          }}
        >
          FlinkSQL Lineage  ©2025
        </Footer>
      </Layout>
    </Layout>
    </ConfigProvider>
  );
}

export default App
