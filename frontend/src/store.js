import { configureStore, createSlice } from '@reduxjs/toolkit';

const bceSlice = createSlice({
  name: 'bce',
  initialState: {
    searchQuery: '',
    searchResults: [],
    searchLoading: false,
    searchError: null,
    
    activeEnterprise: null,
    profileLoading: false,
    profileError: null,
    
    directors: [],
    directorsLoading: false,
    directorsError: null,
    
    statutes: [],
    statutesLoading: false,
    statutesError: null,
    statutesStreaming: false,
  },
  reducers: {
    setSearchQuery: (state, action) => {
      state.searchQuery = action.payload;
    },
    fetchSearchStart: (state) => {
      state.searchLoading = true;
      state.searchError = null;
    },
    fetchSearchSuccess: (state, action) => {
      state.searchLoading = false;
      state.searchResults = action.payload;
    },
    fetchSearchFailure: (state, action) => {
      state.searchLoading = false;
      state.searchError = action.payload;
    },
    
    fetchProfileStart: (state) => {
      state.profileLoading = true;
      state.profileError = null;
      state.activeEnterprise = null;
      state.directors = [];
      state.statutes = [];
      state.statutesStreaming = false;
    },
    fetchProfileSuccess: (state, action) => {
      state.profileLoading = false;
      state.activeEnterprise = action.payload;
    },
    fetchProfileFailure: (state, action) => {
      state.profileLoading = false;
      state.profileError = action.payload;
    },
    
    fetchDirectorsStart: (state) => {
      state.directorsLoading = true;
      state.directorsError = null;
    },
    fetchDirectorsSuccess: (state, action) => {
      state.directorsLoading = false;
      state.directors = action.payload;
    },
    fetchDirectorsFailure: (state, action) => {
      state.directorsLoading = false;
      state.directorsError = action.payload;
    },
    
    clearActiveProfile: (state) => {
      state.activeEnterprise = null;
      state.directors = [];
      state.statutes = [];
      state.statutesStreaming = false;
      state.profileError = null;
      state.directorsError = null;
      state.statutesError = null;
    },
    
    startStatutesStream: (state) => {
      state.statutes = [];
      state.statutesStreaming = true;
      state.statutesError = null;
    },
    addStatuteEvent: (state, action) => {
      const event = action.payload;
      if (event.event === 'downloading') {
        const exists = state.statutes.find(s => s.documentId === event.documentId);
        if (!exists) {
          state.statutes.push({
            documentId: event.documentId,
            deedDate: event.deedDate,
            description: event.description || '',
            status: 'downloading',
            filename: '',
            sizeBytes: 0
          });
        }
      } else if (event.event === 'downloaded') {
        const idx = state.statutes.findIndex(s => s.documentId === event.documentId);
        if (idx !== -1) {
          state.statutes[idx].status = 'downloaded';
          state.statutes[idx].filename = event.filename;
          state.statutes[idx].sizeBytes = event.sizeBytes;
        } else {
          state.statutes.push({
            documentId: event.documentId,
            deedDate: event.deedDate,
            description: event.description || '',
            status: 'downloaded',
            filename: event.filename,
            sizeBytes: event.sizeBytes
          });
        }
      } else if (event.event === 'error') {
        state.statutesError = event.message;
        state.statutesStreaming = false;
      } else if (event.event === 'done') {
        state.statutesStreaming = false;
      }
    },
    stopStatutesStream: (state) => {
      state.statutesStreaming = false;
    }
  }
});

export const {
  setSearchQuery,
  fetchSearchStart,
  fetchSearchSuccess,
  fetchSearchFailure,
  fetchProfileStart,
  fetchProfileSuccess,
  fetchProfileFailure,
  fetchDirectorsStart,
  fetchDirectorsSuccess,
  fetchDirectorsFailure,
  clearActiveProfile,
  startStatutesStream,
  addStatuteEvent,
  stopStatutesStream
} = bceSlice.actions;

export const store = configureStore({
  reducer: {
    bce: bceSlice.reducer
  }
});
