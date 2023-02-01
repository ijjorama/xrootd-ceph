//------------------------------------------------------------------------------
// Copyright (c) 2014-2015 by European Organization for Nuclear Research (CERN)
// Author: Sebastien Ponce <sebastien.ponce@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

#include <stdio.h>
#include <string>
#include <fcntl.h>
#include "XrdCeph/XrdCephPosix.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucName2Name.hh"
#ifdef XRDCEPH_SUBMODULE
#include "XrdOuc/XrdOucN2NLoader.hh"
#else
#include "private/XrdOuc/XrdOucN2NLoader.hh"
#endif
#include "XrdVersion.hh"
#include "XrdCeph/XrdCephOss.hh"
#include "XrdCeph/XrdCephOssDir.hh"
#include "XrdCeph/XrdCephOssFile.hh"

XrdVERSIONINFO(XrdOssGetStorageSystem, XrdCephOss);

XrdSysError XrdCephEroute(0);
XrdOucTrace XrdCephTrace(&XrdCephEroute);

/// timestamp output for logging messages
static std::string ts() {
    std::time_t t = std::time(nullptr);
    char mbstr[50];
    std::strftime(mbstr, sizeof(mbstr), "%y%m%d %H:%M:%S ", std::localtime(&t));
    return std::string(mbstr);
}

// log wrapping function to be used by ceph_posix interface
char g_logstring[1024];
static void logwrapper(char *format, va_list argp) {
  vsnprintf(g_logstring, 1024, format, argp);
  XrdCephEroute.Say(ts().c_str(), g_logstring);
}

/// pointer to library providing Name2Name interface. 0 be default
/// populated in case of ceph.namelib entry in the config file
/// used in XrdCephPosix
extern XrdOucName2Name *g_namelib;

ssize_t getNumericAttr(const char* const path, const char* attrName, const int maxAttrLen)
{

  ssize_t retval;
  char *attrValue = (char*)malloc(maxAttrLen+1);
  if (NULL == attrValue) {
    return -ENOMEM;
  }
  ssize_t attrLen = ceph_posix_getxattr((XrdOucEnv*)NULL, path, attrName, attrValue, maxAttrLen);

  if (attrLen <= 0) {
    retval = -EINVAL;
  } else {
    attrValue[attrLen] = (char)NULL;
    char *endPointer = (char *)NULL;
    retval = strtoll(attrValue, &endPointer, 10);
  }

  if (NULL != attrValue) {
    free(attrValue);
  }
  
  return retval;

}

extern "C"
{
  XrdOss*
  XrdOssGetStorageSystem(XrdOss* native_oss,
                         XrdSysLogger* lp,
                         const char* config_fn,
                         const char* parms)
  {
    // Do the herald thing
    XrdCephEroute.SetPrefix("ceph_");
    XrdCephEroute.logger(lp);
    XrdCephEroute.Say("++++++ CERN/IT-DSS XrdCeph");
    // set parameters
    try {
      ceph_posix_set_defaults(parms);
    } catch (std::exception &e) {
      XrdCephEroute.Say("CephOss loading failed with exception. Check the syntax of parameters : ", parms);
      return 0;
    }
    // deal with logging
    ceph_posix_set_logfunc(logwrapper);
    return new XrdCephOss(config_fn, XrdCephEroute);
  }
}

XrdCephOss::XrdCephOss(const char *configfn, XrdSysError &Eroute) {
  Configure(configfn, Eroute);
}

XrdCephOss::~XrdCephOss() {
  ceph_posix_disconnect_all();
}

// declared and used in XrdCephPosix.cc
extern unsigned int g_maxCephPoolIdx;
int XrdCephOss::Configure(const char *configfn, XrdSysError &Eroute) {
   int NoGo = 0;
   XrdOucEnv myEnv;
   XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"), &myEnv, "=====> ");
   //disable posc  
   XrdOucEnv::Export("XRDXROOTD_NOPOSC", "1");
   // If there is no config file, nothing to be done
   if (configfn && *configfn) {
     // Try to open the configuration file.
     int cfgFD;
     if ((cfgFD = open(configfn, O_RDONLY, 0)) < 0) {
       Eroute.Emsg("Config", errno, "open config file", configfn);
       return 1;
     }
     Config.Attach(cfgFD);
     // Now start reading records until eof.
     char *var;
     while((var = Config.GetMyFirstWord())) {
       if (!strncmp(var, "ceph.nbconnections", 18)) {
         var = Config.GetWord();
         if (var) {
           unsigned long value = strtoul(var, 0, 10);
           if (value > 0 and value <= 100) {
             g_maxCephPoolIdx = value;
           } else {
             Eroute.Emsg("Config", "Invalid value for ceph.nbconnections in config file (must be between 1 and 100)", configfn, var);
             return 1;
           }
         } else {
           Eroute.Emsg("Config", "Missing value for ceph.nbconnections in config file", configfn);
           return 1;
         }
       }
       if (!strncmp(var, "ceph.namelib", 12)) {
         var = Config.GetWord();
         if (var) {
           std::string libname = var;
           // Warn in case parameters were givne
           char parms[1040];
           bool hasParms{false};
           if (!Config.GetRest(parms, sizeof(parms)) || parms[0]) {
              hasParms = true;
           }
           // Load name lib
           XrdOucN2NLoader  n2nLoader(&Eroute,configfn,(hasParms?parms:""),NULL,NULL);
           g_namelib = n2nLoader.Load(libname.c_str(), XrdVERSIONINFOVAR(XrdOssGetStorageSystem), NULL);
           if (!g_namelib) {
             Eroute.Emsg("Config", "Unable to load library given in ceph.namelib : %s", var);
           }
         } else {
           Eroute.Emsg("Config", "Missing value for ceph.namelib in config file ", configfn);
           return 1;
         }
       }

       if (!strcmp(var, "ceph.reportingpools")) {
         var = Config.GetWord();
         if (var) {
           m_configPoolnames = var;
           Eroute.Say("Reporting pools are ", m_configPoolnames.c_str());
         } else {
           Eroute.Emsg("Config", "Missing value for ceph.reportingpools in config file", configfn);
           return 1; 
         }
       }       
 
     }

     // Now check if any errors occured during file i/o
     int retc = Config.LastError();
     if (retc) {
       NoGo = Eroute.Emsg("Config", -retc, "read config file",
                          configfn);
     }
     Config.Close();
   }
   return NoGo;
}

int XrdCephOss::Chmod(const char *path, mode_t mode, XrdOucEnv *envP) {
  return -ENOTSUP;
}

int XrdCephOss::Create(const char *tident, const char *path, mode_t access_mode,
                    XrdOucEnv &env, int Opts) {
  return -ENOTSUP;
}

int XrdCephOss::Init(XrdSysLogger *logger, const char* configFn) { return 0; }

//SCS - lie to posix-assuming clients about directories [fixes brittleness in GFAL2]
int XrdCephOss::Mkdir(const char *path, mode_t mode, int mkpath, XrdOucEnv *envP) {
  return 0;
}

//SCS - lie to posix-assuming clients about directories [fixes brittleness in GFAL2]
int XrdCephOss::Remdir(const char *path, int Opts, XrdOucEnv *eP) {
  return 0;
}

int XrdCephOss::Rename(const char *from,
                    const char *to,
                    XrdOucEnv *eP1,
                    XrdOucEnv *eP2) {
  return -ENOTSUP;
}

int XrdCephOss::Stat(const char* path,
                  struct stat* buff,
                  int opts,
                  XrdOucEnv* env) {

  std::string trimmedPath(path);
  trimmedPath.pop_back(); // Remove trailing colon ':' for comparison against list of pool names

  try {
    if (!strcmp(path, "/")) {
      // special case of a stat made by the locate interface
      // we intend to then list all files 
      // Needed for e.g. FTS, which will stat the first character ('/') of a path
      // before stat-ing the full pathname 
      memset(buff, 0, sizeof(*buff));
      buff->st_mode = S_IFDIR | 0700;
      return XrdOssOK;
    
    } else if (m_configPoolnames.find(trimmedPath) != std::string::npos)  { // Support 'locate' for spaceinfo
#ifdef STAT_TRACE  
      XrdCephEroute.Say("Stat - Disk spaceinfo report for ", path);
#endif
      return XrdOssOK; // Only requires a status code, do not need to fill contents in struct stat

    } else {
        return ceph_posix_stat(env, path, buff);
    }
  } catch (std::exception &e) {
    XrdCephEroute.Say("stat : invalid syntax in file parameters");
    return -EINVAL;
  }
}

int XrdCephOss::StatFS(const char *path, char *buff, int &blen, XrdOucEnv *eP) {
  
  XrdCephEroute.Say("Entering StatFS");
  XrdOssVSInfo sP;
  int rc = StatVS(&sP, 0, 0);
  if (rc) {
    return rc;
  }
  int percentUsedSpace = (sP.Usage*100)/sP.Total;
  blen = snprintf(buff, blen, "%d %lld %d %d %lld %d",
                  1, sP.Free, percentUsedSpace, 0, 0LL, 0);
  return XrdOssOK;
}

int XrdCephOss::StatVS(XrdOssVSInfo *sP, const char *sname, int updt) {

#ifdef STAT_TRACE
  XrdCephEroute.Say("Entering StatVS");
#endif
  int rc = ceph_posix_statfs(&(sP->Total), &(sP->Free));
  if (rc) {
    return rc;
  }
  sP->Large = sP->Total;
  sP->LFree = sP->Free;
  sP->Usage = sP->Total-sP->Free;
  sP->Extents = 1;
  return XrdOssOK;
}

int formatStatLSResponse(char *buff, int &blen, const char* cgroup, long long totalSpace, 
  long long usedSpace, long long freeSpace, long long quota, long long maxFreeChunk)
{
  return snprintf(buff, blen, "oss.cgroup=%s&oss.space=%lld&oss.free=%lld&oss.maxf=%lld&oss.used=%lld&oss.quota=%lld",
                                     cgroup,       totalSpace,    freeSpace,    maxFreeChunk, usedSpace,    quota);
}

int XrdCephOss::StatLS(XrdOucEnv &env, const char *path, char *buff, int &blen)
{
#ifdef STAT_TRACE
  XrdCephEroute.Say("StatLS - Disk space report for ", path);  
#endif
  std::string trimmedPath(path);
  trimmedPath.pop_back(); // Remove trailing colon ':' for comparison against list of pool names

  if (m_configPoolnames.find(trimmedPath) == std::string::npos) {
    XrdCephEroute.Say("Can't report on ", path);
    return -EINVAL;
  }

  long long usedSpace, totalSpace, freeSpace;

  if (ceph_posix_stat_pool(trimmedPath.c_str(), &usedSpace) != 0) {
      XrdCephEroute.Say("Failed to get used space in pool ", trimmedPath.c_str());
      return -EINVAL;
  }

  // Construct the object path
  std::string spaceInfoPath =  trimmedPath + ":" +  (const char *)"__spaceinfo__";
  totalSpace = getNumericAttr(spaceInfoPath.c_str(), "total_space", 24);
  if (totalSpace < 0) {
    XrdCephEroute.Say("Could not get 'total_space' attribute from ", spaceInfoPath.c_str());
    return -EINVAL;
  }

//
// Figure for 'usedSpace' accounts for Erasure Coding overhead
//


  freeSpace = totalSpace - usedSpace;
  blen = formatStatLSResponse(buff, blen, 
    path,       /* "oss.cgroup" */ 
    totalSpace, /* "oss.space"  */
    usedSpace,  /* "oss.used"   */
    freeSpace,  /* "oss.free"   */
    totalSpace, /* "oss.quota"  */
    freeSpace   /* "oss.maxf"   */);

  return XrdOssOK;

}
 
int XrdCephOss::Truncate (const char* path,
                          unsigned long long size,
                          XrdOucEnv* env) {
  try {
    return ceph_posix_truncate(env, path, size);
  } catch (std::exception &e) {
    XrdCephEroute.Say("truncate : invalid syntax in file parameters");
    return -EINVAL;
  }
}

int XrdCephOss::Unlink(const char *path, int Opts, XrdOucEnv *env) {
  try {
    return ceph_posix_unlink(env, path);
  } catch (std::exception &e) {
    XrdCephEroute.Say("unlink : invalid syntax in file parameters");
    return -EINVAL;
  }
}

XrdOssDF* XrdCephOss::newDir(const char *tident) {
  return new XrdCephOssDir(this);
}

XrdOssDF* XrdCephOss::newFile(const char *tident) {
  return new XrdCephOssFile(this);
}

