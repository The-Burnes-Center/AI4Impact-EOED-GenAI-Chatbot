import { useContext } from "react";
import {
  BrowserRouter,
  HashRouter,
  Outlet,
  Route,
  Routes,
  Navigate,
} from "react-router-dom";
import { AppContext } from "./common/app-context";
import GlobalHeader from "./components/global-header";
import Playground from "./pages/chatbot/playground/playground";
import DataPage from "./pages/admin/data-view-page";
import UserFeedbackPage from "./pages/admin/user-feedback-page";
import SessionPage from "./pages/chatbot/sessions/sessions";
import LandingPage from './pages/landing-page/base-page';
import ResourcesPage from './pages/resources-track/resources-page';
import "./styles/app.scss";

function App() {
  const appContext = useContext(AppContext);
  const Router = BrowserRouter;

  return (
    <div style={{ height: "100%" }}>
      <Router>
        <GlobalHeader />
        <div style={{ height: "56px", backgroundColor: "#000716" }}>&nbsp;</div>
        <div>
        <Routes>
            {/* Render LandingPage directly at the root */}
            <Route path="/" element={<LandingPage />} />  

            {/* Render Resources page */}
            <Route path="/resources-track/resources-page" element={<ResourcesPage />} />          
         
            {/* Render Chatbot pages under /chatbot */}
            <Route path="/chatbot" element={<Outlet />}>
              <Route path="playground/:sessionId" element={<Playground />} />
              <Route path="sessions" element={<SessionPage />} />              
            </Route>

            {/* Render Admin pages under /admin */}
            <Route path="/admin" element={<Outlet />}>                 
             <Route path="data" element={<DataPage />} />   
             <Route path="user-feedback" element={<UserFeedbackPage />} />                           
            </Route>     
                   
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </div>
      </Router>
    </div>
  );
}

export default App;
